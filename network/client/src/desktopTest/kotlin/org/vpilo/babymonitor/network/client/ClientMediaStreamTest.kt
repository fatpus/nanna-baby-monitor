package org.vpilo.babymonitor.network.client

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.model.repository.DeviceId
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.transport.VersionMismatchException
import java.io.IOException
import java.net.ProtocolException
import java.security.cert.CertificateException
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.uuid.Uuid

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ClientMediaStreamTest {
    @Test
    fun demandStartsOnlyAfterControlAuthenticationAndLossJoinsOnlyThatMedium() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.transport.autoAuthenticate = false
            fixture.audio.value = true
            fixture.video.value = true
            fixture.connect()
            assertEquals(listOf(Endpoints.CONTROL), fixture.transport.attempts.map { it.endpoint })
            fixture.transport.latest().authenticate()
            runCurrent()
            assertEquals(3, fixture.transport.active.size)
            fixture.audio.value = false
            runCurrent()
            assertEquals(
                setOf(Endpoints.CONTROL, Endpoints.STREAM_VIDEO),
                fixture.transport.active
                    .map { it.endpoint }
                    .toSet(),
            )
            fixture.assertConnected()
            fixture.stop()
            assertTrue(fixture.transport.active.isEmpty())
        }

    @Test
    fun renewedDemandWaitsForOldMediumCleanup() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.audio.value = true
            fixture.connect()
            val audio = fixture.transport.latest(Endpoints.STREAM_AUDIO)
            val gate = CompletableDeferred<Unit>()
            fixture.transport.onCleanup = { if (it === audio) gate.await() }
            fixture.audio.value = false
            runCurrent()
            fixture.audio.value = true
            runCurrent()
            assertEquals(1, fixture.transport.count(Endpoints.STREAM_AUDIO))
            gate.complete(Unit)
            runCurrent()
            assertTrue(audio.closed)
            assertEquals(2, fixture.transport.count(Endpoints.STREAM_AUDIO))
            fixture.stop()
        }

    @Test
    fun recoverableMediaFailureRetriesOnlyThatMediumAtExactlyThreeSeconds() =
        runTest {
            for (endpoint in listOf(Endpoints.STREAM_AUDIO, Endpoints.STREAM_VIDEO)) {
                val fixture = ClientConnectionTestFixture(this)
                fixture.audio.value = true
                fixture.video.value = true
                fixture.connect()
                val failed = fixture.transport.latest(endpoint)
                failed.finish.complete(IOException("Media lost"))
                runCurrent()
                assertTrue(failed.closed)
                assertEquals(2, fixture.transport.active.size)
                advanceTimeBy(2_999)
                runCurrent()
                assertEquals(1, fixture.transport.count(endpoint))
                advanceTimeBy(1)
                runCurrent()
                assertEquals(2, fixture.transport.count(endpoint))
                assertEquals(1, fixture.transport.count(Endpoints.CONTROL))
                assertEquals(3, fixture.transport.active.size)
                fixture.assertConnected()
                fixture.stop()
            }
        }

    @Test
    fun normalMediaReturnRestartsTheStreamAfterTheRetryDelay() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.audio.value = true
            fixture.connect()
            fixture.transport
                .latest(Endpoints.STREAM_AUDIO)
                .finish
                .complete(null)
            runCurrent()

            advanceTimeBy(2_999)
            runCurrent()
            assertEquals(1, fixture.transport.count(Endpoints.STREAM_AUDIO))

            advanceTimeBy(1)
            runCurrent()
            assertEquals(2, fixture.transport.count(Endpoints.STREAM_AUDIO))
            assertEquals(1, fixture.transport.count(Endpoints.CONTROL))
            fixture.assertConnected()
            fixture.stop()
        }

    @Test
    fun demandLossCancelsMediaRetryDelay() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.audio.value = true
            fixture.connect()
            fixture.transport
                .latest(Endpoints.STREAM_AUDIO)
                .finish
                .complete(IOException("Lost"))
            runCurrent()
            fixture.audio.value = false
            runCurrent()
            advanceTimeBy(3_000)
            runCurrent()
            assertEquals(1, fixture.transport.count(Endpoints.STREAM_AUDIO))
            fixture.audio.value = true
            runCurrent()
            assertEquals(2, fixture.transport.count(Endpoints.STREAM_AUDIO))
            fixture.stop()
        }

    @Test
    fun terminalFailureFromEitherMediumJoinsHealthySiblingsAndPreservesMapping() =
        runTest {
            val failures =
                listOf(
                    PairingRevokedException("Revoked") to ErrorReason.PairingRevoked,
                    CertificateException("Changed") to ErrorReason.CertificateMismatch,
                    ProtocolException("Protocol") to ErrorReason.ServerQuit,
                    VersionMismatchException(1, 2, false) to ErrorReason.MonitorDeviceOutdated,
                    VersionMismatchException(2, 1, false) to ErrorReason.CameraDeviceOutdated,
                    VersionMismatchException(1, 2, true) to ErrorReason.AppRelayOutdated,
                    VersionMismatchException(2, 1, true) to ErrorReason.RelayOutdated,
                )
            for (endpoint in listOf(Endpoints.STREAM_AUDIO, Endpoints.STREAM_VIDEO)) {
                for ((failure, reason) in failures) {
                    val fixture = ClientConnectionTestFixture(this)
                    fixture.audio.value = true
                    fixture.video.value = true
                    fixture.connect()
                    fixture.transport
                        .latest(endpoint)
                        .finish
                        .complete(failure)
                    runCurrent()
                    fixture.assertReason(reason)
                    assertTrue(fixture.transport.active.isEmpty())
                    assertEquals(1, fixture.links.single().stops)
                    assertEquals(if (failure is PairingRevokedException || failure is CertificateException) 1 else 0, fixture.unpairCalls)
                    fixture.stop()
                }
            }
        }

    @Test
    fun cancellationWrappedTerminalMediaFailureIsNotMistakenForDemandCancellation() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.audio.value = true
            fixture.connect()
            fixture.transport
                .latest(
                    Endpoints.STREAM_AUDIO,
                ).finish
                .complete(CancellationException("Wrapped", PairingRevokedException("Revoked")))
            runCurrent()
            fixture.assertReason(ErrorReason.PairingRevoked)
            assertTrue(fixture.transport.active.isEmpty())
            fixture.stop()
        }

    @Test
    fun terminalMediaFailureWinsRecoverableControlCleanupFailure() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.audio.value = true
            fixture.connect()
            fixture.transport.onCleanup = { if (it.endpoint == Endpoints.CONTROL) throw IOException("Control close raced") }
            fixture.transport
                .latest(Endpoints.STREAM_AUDIO)
                .finish
                .complete(CertificateException("Changed"))
            runCurrent()
            fixture.assertReason(ErrorReason.CertificateMismatch)
            assertEquals(1, fixture.unpairCalls)
            fixture.stop()
        }

    @Test
    fun normalAndExceptionalControlReturnJoinMediaBeforeRetryAndRestoreCurrentDemand() =
        runTest {
            for (failure in listOf(null, IOException("Dropped"))) {
                val fixture = ClientConnectionTestFixture(this)
                fixture.audio.value = true
                fixture.video.value = true
                fixture.connect()
                val media = fixture.transport.latest(Endpoints.STREAM_AUDIO)
                val gate = CompletableDeferred<Unit>()
                fixture.transport.onCleanup = { if (it === media) gate.await() }
                fixture.transport
                    .latest()
                    .finish
                    .complete(failure)
                runCurrent()
                advanceTimeBy(3_000)
                runCurrent()
                assertEquals(1, fixture.transport.count(Endpoints.CONTROL))
                fixture.video.value = false
                gate.complete(Unit)
                runCurrent()
                assertTrue(fixture.transport.active.isEmpty())
                advanceTimeBy(3_000)
                runCurrent()
                assertEquals(2, fixture.transport.count(Endpoints.CONTROL))
                assertEquals(2, fixture.transport.count(Endpoints.STREAM_AUDIO))
                assertEquals(1, fixture.transport.count(Endpoints.STREAM_VIDEO))
                fixture.assertConnected()
                fixture.stop()
            }
        }

    @Test
    fun replacedTargetCannotDeliverMoreEncodedChunksAfterReplacementCompletes() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            val delivered = mutableListOf<DeviceId>()
            fixture.video.value = true
            fixture.transport.read = { attempt ->
                if (attempt.endpoint == Endpoints.STREAM_VIDEO) {
                    while (true) {
                        delivered += attempt.target.id
                        delay(10)
                    }
                } else {
                    attempt.finish.await()?.let { throw it }
                }
            }
            fixture.connect()
            advanceTimeBy(20)
            runCurrent()
            assertTrue(delivered.contains(fixture.target.id))
            val next = Device.LocalServer(Uuid.random(), "Next", NetworkAddress("10.0.0.2"))
            fixture.repository.connect(next)
            runCurrent()
            assertEquals(ConnectionState.Connected(next), fixture.repository.connectionStateFlow.first())
            val afterReplacement = delivered.size
            advanceTimeBy(100)
            runCurrent()
            assertTrue(delivered.drop(afterReplacement).isNotEmpty())
            assertTrue(delivered.drop(afterReplacement).all { it == next.id })
            fixture.stop()
        }

    @Test
    fun programmingFailureInMediaEndsTheLifetimeInsteadOfRetryingForever() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.audio.value = true
            fixture.connect()
            fixture.transport
                .latest(Endpoints.STREAM_AUDIO)
                .finish
                .complete(IllegalStateException("Reader defect"))
            runCurrent()
            fixture.assertReason(ErrorReason.ServerNotFound)
            advanceTimeBy(30_000)
            runCurrent()
            assertEquals(1, fixture.transport.count(Endpoints.STREAM_AUDIO))
            assertTrue(fixture.transport.active.isEmpty())
            fixture.stop()
        }
}
