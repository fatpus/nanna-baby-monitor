package org.vpilo.babymonitor.network.client

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.network.model.RelayConfiguration
import java.io.IOException
import java.net.ProtocolException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.uuid.Uuid

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ClientConnectionLifecycleTest {
    @Test
    fun disconnectAndResetCancelEverySuspendedSetupStage() =
        runTest {
            for (stage in listOf("identity", "pairing", "relay")) {
                for (reset in listOf(false, true)) {
                    val fixture = ClientConnectionTestFixture(this)
                    val gate = CompletableDeferred<Unit>()
                    when (stage) {
                        "identity" -> {
                            fixture.loadIdentity = {
                                gate.await()
                                fixture.localDevice
                            }
                        }

                        "pairing" -> {
                            fixture.findPairing = {
                                gate.await()
                                fixture.pairing
                            }
                        }

                        "relay" -> {
                            fixture.relayFlow =
                                flow {
                                    gate.await()
                                    emitAll(fixture.relay)
                                }
                        }
                    }
                    val request = launch { fixture.repository.connect(fixture.target) }
                    runCurrent()
                    assertTrue(request.isCompleted, "acceptance must precede $stage loading")
                    assertEquals(1, fixture.links.single().starts)
                    fixture.stop(reset)
                    gate.complete(Unit)
                    runCurrent()
                    assertEquals(0, fixture.transport.attempts.size)
                    assertEquals(1, fixture.links.single().stops)
                    fixture.assertReason(if (reset) ErrorReason.NotConnectedYet else ErrorReason.ClientQuit)
                    fixture.loadIdentity = { fixture.localDevice }
                    fixture.findPairing = { fixture.pairing }
                    fixture.relayFlow = fixture.relay
                    fixture.connect()
                    fixture.assertConnected()
                    fixture.stop()
                }
            }
        }

    @Test
    fun injectedJobAndRequestCancellationDoNotOwnTheConnection() =
        runTest {
            val injected = Job()
            val fixture = ClientConnectionTestFixture(this, injected)
            val request =
                launch {
                    fixture.repository.connect(fixture.target)
                    awaitCancellation()
                }
            runCurrent()
            request.cancel()
            injected.cancel()
            runCurrent()
            fixture.assertConnected()
            fixture.stop()
        }

    @Test
    fun aRequestCancelledBeforeAdmissionHasNoEffect() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            val request = launch(start = CoroutineStart.UNDISPATCHED) { fixture.repository.connect(fixture.target) }
            request.cancel()
            runCurrent()
            assertTrue(fixture.transport.attempts.isEmpty())
            assertTrue(fixture.links.isEmpty())
            fixture.stop()
        }

    @Test
    fun replacementAcknowledgesAcceptanceButJoinsOldCleanupBeforeNewSetup() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            val old = fixture.transport.latest()
            val cleanup = CompletableDeferred<Unit>()
            fixture.transport.onCleanup = { if (it === old) cleanup.await() }
            val replacement = Device.LocalServer(Uuid.random(), "New camera", NetworkAddress("10.0.0.2"))
            val request =
                launch {
                    fixture.repository.connect(replacement)
                    awaitCancellation()
                }
            runCurrent()
            assertEquals(1, fixture.identityLoads)
            assertFalse(old.closed)
            request.cancel()
            cleanup.complete(Unit)
            runCurrent()
            assertTrue(old.closed)
            assertEquals(2, fixture.identityLoads)
            assertEquals(replacement, fixture.transport.latest().target)
            assertEquals(ConnectionState.Connected(replacement), fixture.repository.connectionStateFlow.first())
            assertEquals(1, fixture.links.first().stops)
            fixture.stop()
        }

    @Test
    fun replacementDoesNotPublishDisconnectedBetweenTargets() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            val states = mutableListOf<ConnectionState>()
            val observer =
                backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
                    fixture.repository.connectionStateFlow.collect { states += it }
                }
            fixture.repository.connect(Device.LocalServer(Uuid.random(), "Other", NetworkAddress("10.0.0.2")))
            runCurrent()
            assertTrue(states.none { it is ConnectionState.Disconnected })
            observer.cancel()
            fixture.stop()
        }

    @Test
    fun queuedCancelledRequestsAreSkippedAndStopConnectOrderingSurvivesTeardown() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            val gate = CompletableDeferred<Unit>()
            fixture.transport.onCleanup = { gate.await() }
            val firstStop = launch { fixture.repository.disconnect() }
            runCurrent()
            assertFalse(firstStop.isCompleted)
            val cancelled =
                launch(start = CoroutineStart.UNDISPATCHED) {
                    fixture.repository.connect(Device.LocalServer(Uuid.random(), "Cancelled", NetworkAddress("10.0.0.3")))
                }
            cancelled.cancel()
            val later = Device.LocalServer(Uuid.random(), "Later", NetworkAddress("10.0.0.4"))
            val connect = launch(start = CoroutineStart.UNDISPATCHED) { fixture.repository.connect(later) }
            gate.complete(Unit)
            runCurrent()
            assertTrue(firstStop.isCompleted)
            assertTrue(connect.isCompleted)
            assertEquals(listOf(fixture.target, later), fixture.transport.attempts.map { it.target })
            fixture.stop()
        }

    @Test
    fun displayNameIsIgnoredButAddressAndRouteChangesReplace() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            fixture.repository.connect(Device.LocalServer(fixture.target.id, "Renamed", fixture.target.address))
            runCurrent()
            assertEquals(1, fixture.transport.attempts.size)
            val moved = Device.LocalServer(fixture.target.id, "Moved", NetworkAddress("10.0.0.2"))
            fixture.repository.connect(moved)
            runCurrent()
            assertEquals(2, fixture.transport.attempts.size)
            fixture.repository.connect(Device.RemoteServer(moved.id, moved.name, moved.address))
            runCurrent()
            assertEquals(3, fixture.transport.attempts.size)
            assertTrue(
                fixture.transport.attempts
                    .take(2)
                    .all { it.closed },
            )
            fixture.stop()
        }

    @Test
    fun repeatedTargetDuringSetupIsANoopAndCompletedTargetCanRestart() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            val gate = CompletableDeferred<Unit>()
            fixture.loadIdentity = {
                gate.await()
                fixture.localDevice
            }
            fixture.repository.connect(fixture.target)
            runCurrent()
            fixture.repository.connect(fixture.target)
            runCurrent()
            assertEquals(1, fixture.identityLoads)
            gate.complete(Unit)
            runCurrent()
            fixture.transport
                .latest()
                .finish
                .complete(ProtocolException("Ended"))
            runCurrent()
            fixture.assertReason(ErrorReason.ServerQuit)
            fixture.connect()
            assertEquals(2, fixture.transport.attempts.size)
            fixture.stop()
        }

    @Test
    fun setupAndPartialRegistrationFailuresReleaseTheLinkAndLeaveConnecting() =
        runTest {
            for (stage in listOf("foreground", "identity", "pairing", "relay")) {
                val fixture = ClientConnectionTestFixture(this)
                when (stage) {
                    "foreground" -> fixture.startFailure = IOException("Registration partially acquired")
                    "identity" -> fixture.loadIdentity = { throw IOException("Identity failed") }
                    "pairing" -> fixture.findPairing = { throw IOException("Pairing failed") }
                    "relay" -> fixture.relayFlow = flow { throw IOException("Settings failed") }
                }
                fixture.connect()
                fixture.assertReason(ErrorReason.ServerNotFound)
                assertEquals(1, fixture.links.single().stops)
                assertTrue(fixture.transport.attempts.isEmpty())
                fixture.stop()
            }
        }

    @Test
    fun missingPairingIsRevokedAndStorageFailurePreservesTheReason() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.findPairing = { null }
            fixture.unpair = { throw IOException("Storage unavailable") }
            fixture.connect()
            fixture.assertReason(ErrorReason.PairingRevoked)
            assertEquals(1, fixture.unpairCalls)
            assertEquals(1, fixture.links.single().stops)
            fixture.stop()
        }

    @Test
    fun terminalUnpairingIsAwaitedByReplacementAndDisconnect() =
        runTest {
            for (replace in listOf(false, true)) {
                val fixture = ClientConnectionTestFixture(this)
                val unpairGate = CompletableDeferred<Unit>()
                fixture.unpair = { unpairGate.await() }
                fixture.connect()
                fixture.transport
                    .latest()
                    .finish
                    .complete(PairingRevokedException("Revoked"))
                runCurrent()
                assertEquals(1, fixture.unpairCalls)
                val next = Device.LocalServer(Uuid.random(), "Next", NetworkAddress("10.0.0.2"))
                val request = launch { if (replace) fixture.repository.connect(next) else fixture.repository.disconnect() }
                runCurrent()
                if (replace) assertTrue(request.isCompleted) else assertFalse(request.isCompleted)
                assertEquals(1, fixture.identityLoads)
                unpairGate.complete(Unit)
                runCurrent()
                assertTrue(request.isCompleted)
                if (replace) {
                    assertEquals(ConnectionState.Connected(next), fixture.repository.connectionStateFlow.first())
                    fixture.stop()
                } else {
                    fixture.assertReason(ErrorReason.ClientQuit)
                }
            }
        }

    @Test
    fun relayChangeDuringIdentitySetupOrActiveControlEndsTheRemoteLifetime() =
        runTest {
            for (duringSetup in listOf(false, true)) {
                val fixture = ClientConnectionTestFixture(this)
                val gate = CompletableDeferred<Unit>()
                if (duringSetup) {
                    fixture.loadIdentity = {
                        gate.await()
                        fixture.localDevice
                    }
                }
                val remote = Device.RemoteServer(fixture.target.id, "Remote", fixture.target.address)
                fixture.repository.connect(remote)
                runCurrent()
                fixture.relay.value = RelayConfiguration("new-relay", "new-passphrase")
                runCurrent()
                gate.complete(Unit)
                runCurrent()
                fixture.assertReason(ErrorReason.ClientQuit)
                assertEquals(if (duringSetup) 0 else 1, fixture.transport.attempts.size)
                assertTrue(fixture.transport.attempts.all { it.closed })
                assertEquals(1, fixture.links.single().stops)
                fixture.stop()
            }
        }

    @Test
    fun platformCallbacksAreNonblockingAndOldOrReleaseCallbacksCannotStopReplacement() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.callbackOnRelease = true
            fixture.connect()
            val oldLink = fixture.links.single()
            val gate = CompletableDeferred<Unit>()
            fixture.transport.onCleanup = { gate.await() }
            oldLink.onStopped()
            runCurrent()
            assertEquals(0, oldLink.stops)
            gate.complete(Unit)
            runCurrent()
            fixture.assertReason(ErrorReason.ClientQuit)
            fixture.connect()
            oldLink.onStopped()
            runCurrent()
            fixture.assertConnected()
            assertEquals(1, oldLink.stops)
            fixture.stop()
        }

    @Test
    fun delayedPlatformNotificationCannotOverwriteANaturallyCompletedReason() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            fixture.transport
                .latest()
                .finish
                .complete(ProtocolException("Protocol ended"))
            runCurrent()
            fixture.assertReason(ErrorReason.ServerQuit)
            fixture.links.single().onStopped()
            runCurrent()
            fixture.assertReason(ErrorReason.ServerQuit)
            fixture.stop()
        }
}
