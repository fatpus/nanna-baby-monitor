package org.vpilo.babymonitor.network.security.relay

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class RelayClientCleanupTest {
    @Test
    fun failedRelaySetupAwaitsClientAndEngineCleanupBeforeThrowingOriginalFailure() =
        runTest {
            val client = HttpClient(CIO)
            val engineCleanup = CompletableDeferred<Unit>()
            val clientClosed = CompletableDeferred<Unit>()
            client.coroutineContext.job.invokeOnCompletion { clientClosed.complete(Unit) }
            val cleanup =
                CoroutineScope(client.engine.coroutineContext).launch(start = CoroutineStart.UNDISPATCHED) {
                    try {
                        awaitCancellation()
                    } finally {
                        withContext(NonCancellable) { engineCleanup.await() }
                    }
                }
            val original = IllegalStateException("Relay access handshake failed")
            try {
                val operation =
                    async {
                        runCatching { client.useAwaitingClose { throw original } }.exceptionOrNull()
                    }
                clientClosed.await()
                runCurrent()
                assertFalse(operation.isCompleted, "Relay setup must await outstanding engine cleanup")
                engineCleanup.complete(Unit)
                assertSame(original, operation.await())
                assertTrue(client.coroutineContext.job.isCompleted)
                assertTrue(client.engine.coroutineContext.job.isCompleted)
                assertTrue(cleanup.isCompleted)
            } finally {
                engineCleanup.complete(Unit)
                client.close()
            }
        }

    @Test
    fun cancellationDuringRelaySetupAwaitsClientAndEngineCleanup() =
        runTest {
            val client = HttpClient(CIO)
            val engineCleanup = CompletableDeferred<Unit>()
            val setupStarted = CompletableDeferred<Unit>()
            val clientClosed = CompletableDeferred<Unit>()
            client.coroutineContext.job.invokeOnCompletion { clientClosed.complete(Unit) }
            CoroutineScope(client.engine.coroutineContext).launch(start = CoroutineStart.UNDISPATCHED) {
                try {
                    awaitCancellation()
                } finally {
                    withContext(NonCancellable) { engineCleanup.await() }
                }
            }
            try {
                val operation =
                    async {
                        client.useAwaitingClose {
                            setupStarted.complete(Unit)
                            awaitCancellation()
                        }
                    }
                setupStarted.await()
                operation.cancel()
                clientClosed.await()
                runCurrent()
                assertFalse(operation.isCompleted, "Cancelled setup must await outstanding engine cleanup")
                engineCleanup.complete(Unit)
                operation.join()
                assertTrue(operation.isCancelled)
                assertTrue(client.coroutineContext.job.isCompleted)
                assertTrue(client.engine.coroutineContext.job.isCompleted)
            } finally {
                engineCleanup.complete(Unit)
                client.close()
            }
        }
}
