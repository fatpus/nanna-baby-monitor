package org.vpilo.babymonitor.network.client

import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.network.tls.certificates.buildKeyStore
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.WebSocketSession
import io.ktor.websocket.close
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.DeviceId
import org.vpilo.babymonitor.network.internal.repository.InternalActiveSessionsRepository
import org.vpilo.babymonitor.network.model.Constants
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.RelayConfiguration
import org.vpilo.babymonitor.network.model.pairing.PairedDevice
import org.vpilo.babymonitor.network.model.repository.PairingStorageRepository
import org.vpilo.babymonitor.network.model.transport.VersionMismatch
import org.vpilo.babymonitor.network.model.transport.VersionMismatchException
import org.vpilo.babymonitor.network.security.crypto.computeServerHandshakeProof
import org.vpilo.babymonitor.network.security.crypto.generateSessionSalt
import org.vpilo.babymonitor.network.security.crypto.sha256Fingerprint
import org.vpilo.babymonitor.network.security.protocol.receiveSessionHandshakeRequestOrNull
import org.vpilo.babymonitor.network.security.protocol.sendSessionHandshakeResponse
import java.net.ProtocolException
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import kotlin.coroutines.cancellation.CancellationException
import kotlin.io.encoding.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.uuid.Uuid

class WebSocketConnectionHandlerTest {
    @Test
    fun policyClosureIsPairingRevokedAfterReaderReturns() =
        runBlocking {
            val failure = closeFailure(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "Unpaired"), genericProtocolFailure = true)
            assertIs<PairingRevokedException>(failure)
            Unit
        }

    @Test
    fun protocolClosurePreservesIncompatibilityAfterReaderConsumesException() =
        runBlocking {
            val failure = closeFailure(CloseReason(CloseReason.Codes.PROTOCOL_ERROR, "Unsupported"))
            assertIs<ProtocolException>(failure)
            Unit
        }

    @Test
    fun deviceVersionMismatchIdentifiesOutdatedMonitor() =
        runBlocking {
            assertVersionFailure(4000, "2", VersionMismatch.LOCAL_OUTDATED, false)
        }

    @Test
    fun deviceVersionMismatchIdentifiesOutdatedCamera() =
        runBlocking {
            assertVersionFailure(4000, "0", VersionMismatch.REMOTE_OUTDATED, false)
        }

    @Test
    fun relayVersionMismatchIdentifiesOutdatedApp() =
        runBlocking {
            assertVersionFailure(4001, "2", VersionMismatch.LOCAL_OUTDATED, true)
        }

    @Test
    fun relayVersionMismatchIdentifiesOutdatedRelay() =
        runBlocking {
            assertVersionFailure(4001, "0", VersionMismatch.REMOTE_OUTDATED, true)
        }

    @Test
    fun certificateMismatchSurvivesTlsWrapping() =
        runBlocking {
            withPeer {
                assertFailsWith<CertificateException> {
                    handler(expectedFingerprint = "00".repeat(32)) { }.run()
                }
            }
        }

    @Test
    fun normalReaderReturnClosesHealthySessionBeforeReturning() =
        runBlocking {
            withPeer {
                var sessionJob: Job? = null
                var clientJob: Job? = null
                var engineJob: Job? = null
                handler {
                    sessionJob = this.coroutineContext[Job]
                    assertTrue(sessionJob !== currentCoroutineContext().job, "Cleanup must join the session, never its caller")
                    clientJob = call.client.coroutineContext.job
                    engineJob = call.client.engine.coroutineContext.job
                }.run()
                assertTrue(requireNotNull(sessionJob).isCompleted)
                assertTrue(requireNotNull(clientJob).isCompleted)
                assertTrue(requireNotNull(engineJob).isCompleted)
                peerFinished.await()
            }
        }

    @Test
    fun readerFailureClosesHealthySessionAndPreservesOriginalTerminalCause() =
        runBlocking {
            withPeer {
                val original = PairingRevokedException("Originating media failure")
                val failure = runCatching { handler { throw original }.run() }.exceptionOrNull()
                assertSame(original, failure)
                peerFinished.await()
            }
        }

    @Test
    fun structuredChildFailureClosesHealthySessionAndWinsRecoverableClose() =
        runBlocking {
            withPeer {
                val original = CertificateException("Originating child failure")
                val failure =
                    runCatching {
                        handler {
                            coroutineScope {
                                launch { throw original }
                                incoming.consumeEach { }
                            }
                        }.run()
                    }.exceptionOrNull()
                assertOriginalTerminalFailure(original, failure)
                peerFinished.await()
            }
        }

    @Test
    fun terminalChildFailureWinsConcurrentRecoverablePeerClose() =
        runBlocking {
            val readerStarted = CompletableDeferred<Unit>()
            withPeer(peer = {
                readerStarted.await()
                close(CloseReason(CloseReason.Codes.GOING_AWAY, "Recoverable control drop"))
            }) {
                val original = PairingRevokedException("Media was revoked during control drop")
                val failure =
                    runCatching {
                        handler {
                            coroutineScope {
                                launch {
                                    readerStarted.complete(Unit)
                                    throw original
                                }
                                incoming.consumeEach { }
                            }
                        }.run()
                    }.exceptionOrNull()
                assertOriginalTerminalFailure(original, failure)
                peerFinished.await()
            }
        }

    @Test
    fun callerCancellationJoinsSessionCleanup() =
        runBlocking {
            withPeer {
                val started = CompletableDeferred<Job>()
                val operation =
                    async {
                        handler {
                            started.complete(requireNotNull(coroutineContext[Job]))
                            awaitCancellation()
                        }.run()
                    }
                val sessionJob = started.await()
                operation.cancelAndJoin()
                assertFailsWith<CancellationException> { operation.await() }
                assertTrue(sessionJob.isCompleted)
                peerFinished.await()
            }
        }

    @Test
    fun authenticationCallbackFailureUnregistersProductionControlSession() =
        runBlocking {
            val secret = ByteArray(32) { it.toByte() }
            withPeer(peer = {
                incoming.receive()
                val request = requireNotNull(receiveSessionHandshakeRequestOrNull())
                val salt = generateSessionSalt()
                sendSessionHandshakeResponse(salt, computeServerHandshakeProof(secret, request.salt, salt))
                incoming.consumeEach { }
            }) {
                val active = mutableSetOf<WebSocketSession>()
                val paired = PairedDevice(device.id.toString(), device.name, fingerprint, Base64.encode(secret))
                val storage =
                    object : PairingStorageRepository {
                        override val pairedDevices = MutableStateFlow(listOf(paired))

                        override suspend fun find(deviceId: DeviceId) = paired

                        override suspend fun pair(device: PairedDevice) = error("Unexpected pairing")

                        override suspend fun unpair(deviceId: DeviceId) = error("Unexpected unpairing")

                        override suspend fun updateName(
                            deviceId: DeviceId,
                            newName: String,
                        ) = error("Unexpected rename")
                    }
                val ledger =
                    object : InternalActiveSessionsRepository {
                        override fun register(
                            clientId: DeviceId,
                            session: WebSocketSession,
                        ) {
                            active.add(session)
                        }

                        override fun unregister(
                            clientId: DeviceId,
                            session: WebSocketSession,
                        ) {
                            active.remove(session)
                        }

                        override suspend fun closeSessions(
                            clientId: DeviceId,
                            wasUnpaired: Boolean,
                        ) = Unit
                    }
                val original = PairingRevokedException("Authenticated owner failed")
                val transport =
                    DefaultClientConnectionTransport(
                        NetworkControlDataSource(),
                        NetworkAudioDataSource(),
                        NetworkVideoDataSource(),
                        storage,
                        ledger,
                    )
                val failure =
                    runCatching {
                        transport.run(
                            device,
                            ClientConnectionSetup(Device.Client(Uuid.random(), "Monitor"), fingerprint, RelayConfiguration("", "")),
                            Endpoints.CONTROL,
                        ) {
                            assertEquals(1, active.size)
                            throw original
                        }
                    }.exceptionOrNull()
                assertSame(original, failure)
                assertTrue(active.isEmpty())
                peerFinished.await()
            }
        }

    @Test
    fun suppressedTerminalFailureOutranksRecoverableTeardownException() {
        val original = PairingRevokedException("Media revoked")
        val teardown =
            CancellationException("Reader cancelled").apply {
                initCause(java.net.ConnectException("Control closed"))
                addSuppressed(original)
            }
        assertSame(original, teardown.terminalConnectionFailureOrNull())
    }

    private fun assertOriginalTerminalFailure(
        original: Throwable,
        failure: Throwable?,
    ) {
        val actual = assertNotNull(failure)
        assertEquals(original::class, actual::class)
        assertEquals(original.message, actual.message)
        // Coroutine stack-trace recovery may copy an exception at a child boundary,
        // retaining the original as its cause. Preserve that cause, not wrapper identity.
        val pending = ArrayDeque<Throwable>()
        val seen = mutableSetOf<Throwable>()
        pending.add(actual)
        while (pending.isNotEmpty()) {
            val candidate = pending.removeFirst()
            if (candidate === original) return
            if (!seen.add(candidate)) continue
            candidate.cause?.let(pending::addLast)
            candidate.suppressedExceptions.forEach(pending::addLast)
        }
        fail("The originating terminal failure must remain reachable")
    }

    private suspend fun assertVersionFailure(
        code: Short,
        message: String,
        mismatch: VersionMismatch,
        relay: Boolean,
    ) {
        // The specific version close must override a generic protocol failure from a reader.
        val failure = assertIs<VersionMismatchException>(closeFailure(CloseReason(code, message), genericProtocolFailure = true))
        assertEquals(mismatch, failure.mismatch)
        assertEquals(relay, failure.isRelayConnection)
    }

    private suspend fun closeFailure(
        reason: CloseReason,
        genericProtocolFailure: Boolean = false,
    ): Throwable? {
        var failure: Throwable? = null
        val readerStarted = CompletableDeferred<Unit>()
        withPeer(peer = {
            readerStarted.await()
            close(reason)
        }) {
            failure =
                runCatching {
                    handler {
                        readerStarted.complete(Unit)
                        // Deliberately consume a closed-channel exception like the existing readers.
                        runCatching { while (true) incoming.receive() }
                        if (genericProtocolFailure) throw ProtocolException("Reader saw a generic protocol closure")
                    }.run()
                }.exceptionOrNull()
        }
        return failure
    }

    private suspend fun withPeer(
        peer: suspend DefaultWebSocketServerSession.() -> Unit = { incoming.consumeEach { } },
        test: suspend Peer.() -> Unit,
    ) {
        val fixture = Peer(peer)
        try {
            withTimeout(5_000) { fixture.test() }
        } finally {
            fixture.server.stop(0, 0)
        }
    }

    private class Peer(
        peer: suspend DefaultWebSocketServerSession.() -> Unit,
    ) {
        val peerFinished = CompletableDeferred<Unit>()
        val device = Device.LocalServer(Uuid.random(), "Camera", NetworkAddress("127.0.0.1"))
        val server =
            embeddedServer(
                factory = Netty,
                configure = {
                    sslConnector(
                        keyStore = keyStore,
                        keyAlias = "camera",
                        keyStorePassword = { "test-password".toCharArray() },
                        privateKeyPassword = { "test-password".toCharArray() },
                    ) {
                        host = "127.0.0.1"
                        port = Constants.SERVICE_PORT
                    }
                },
                module = {
                    install(WebSockets)
                    routing {
                        webSocket(Endpoints.CONTROL) {
                            try {
                                peer()
                            } finally {
                                peerFinished.complete(Unit)
                            }
                        }
                    }
                },
            ).start(wait = false)

        fun handler(
            expectedFingerprint: String = fingerprint,
            block: suspend DefaultClientWebSocketSession.() -> Unit,
        ) = WebSocketConnectionHandler(
            device = device,
            endpointPath = Endpoints.CONTROL,
            expectedFingerprint = expectedFingerprint,
            sessionBlock = block,
        )
    }

    private companion object {
        private val keyStore =
            buildKeyStore {
                certificate("camera") {
                    password = "test-password"
                    domains = listOf(Constants.TLS_SERVER_NAME, "127.0.0.1")
                    keySizeInBits = 2048
                    daysValid = 1
                }
            }
        private val fingerprint = (keyStore.getCertificate("camera") as X509Certificate).sha256Fingerprint()
    }
}
