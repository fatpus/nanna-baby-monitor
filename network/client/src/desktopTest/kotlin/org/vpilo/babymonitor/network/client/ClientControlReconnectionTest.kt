package org.vpilo.babymonitor.network.client

import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.network.model.Endpoints
import java.io.IOException
import java.net.ProtocolException
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ClientControlReconnectionTest {
    @Test
    fun initialControlFailureDoesNotRetryAndACompletedTargetCanConnectAgain() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.transport.autoAuthenticate = false
            fixture.connect()
            fixture.transport
                .latest()
                .finish
                .complete(IOException("Unreachable"))
            runCurrent()
            fixture.assertReason(ErrorReason.ServerNotFound)
            advanceTimeBy(30_000)
            runCurrent()
            assertEquals(1, fixture.transport.attempts.size)
            fixture.transport.autoAuthenticate = true
            fixture.connect()
            fixture.assertConnected()
            fixture.stop()
        }

    @Test
    fun threeFailedRetriesExhaustBudgetAndRepeatedTargetDoesNotRestoreIt() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            fixture.transport.autoAuthenticate = false
            fixture.transport
                .latest()
                .finish
                .complete(IOException("Lost"))
            runCurrent()
            for (retry in 1..3) {
                fixture.repository.connect(fixture.target)
                advanceTimeBy(2_999)
                runCurrent()
                assertEquals(retry, fixture.transport.count(Endpoints.CONTROL))
                advanceTimeBy(1)
                runCurrent()
                assertEquals(retry + 1, fixture.transport.count(Endpoints.CONTROL))
                fixture.transport
                    .latest()
                    .finish
                    .complete(IOException("Retry failed"))
                runCurrent()
            }
            fixture.assertReason(ErrorReason.ServerNotFound)
            assertEquals(4, fixture.transport.attempts.size)
            fixture.stop()
        }

    @Test
    fun reauthenticationRestoresBudgetAndDisconnectCancelsRetryDelay() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            repeat(2) {
                fixture.transport.autoAuthenticate = false
                fixture.transport
                    .latest()
                    .finish
                    .complete(IOException("Lost"))
                runCurrent()
                repeat(2) {
                    advanceTimeBy(3_000)
                    runCurrent()
                    fixture.transport
                        .latest()
                        .finish
                        .complete(IOException("Failed retry"))
                    runCurrent()
                }
                fixture.transport.autoAuthenticate = true
                advanceTimeBy(3_000)
                runCurrent()
                fixture.assertConnected()
            }
            assertEquals(7, fixture.transport.count(Endpoints.CONTROL))
            fixture.transport
                .latest()
                .finish
                .complete(IOException("Lost again"))
            runCurrent()
            fixture.stop()
            advanceTimeBy(30_000)
            runCurrent()
            assertEquals(7, fixture.transport.count(Endpoints.CONTROL))
            assertTrue(fixture.transport.active.isEmpty())
        }

    @Test
    fun cancellationWrappedControlFailureRetainsItsTerminalReason() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            fixture.transport
                .latest()
                .finish
                .complete(CancellationException("Wrapped", ProtocolException("Incompatible")))
            runCurrent()
            fixture.assertReason(ErrorReason.ServerQuit)
            assertTrue(fixture.transport.active.isEmpty())
            fixture.stop()
        }
}
