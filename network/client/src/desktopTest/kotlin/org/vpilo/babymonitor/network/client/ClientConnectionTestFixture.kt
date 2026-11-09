package org.vpilo.babymonitor.network.client

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.withContext
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.model.repository.DeviceId
import org.vpilo.babymonitor.model.repository.LocalClientDeviceRepository
import org.vpilo.babymonitor.network.internal.BackgroundServiceLink
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.RelayConfiguration
import org.vpilo.babymonitor.network.model.pairing.PairedDevice
import org.vpilo.babymonitor.network.model.repository.PairingStorageRepository
import org.vpilo.babymonitor.network.model.repository.RelayConfigurationRepository
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.uuid.Uuid

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
internal class ClientConnectionTestFixture(
    val scope: TestScope,
    contextJob: Job = Job(),
    executionDispatcher: CoroutineDispatcher = StandardTestDispatcher(scope.testScheduler),
) {
    class Attempt(
        val target: Device.Server,
        val endpoint: String,
        val authenticate: () -> Unit,
    ) {
        val finish = CompletableDeferred<Throwable?>()
        var closed = false
    }

    class RecordingTransport : ClientConnectionTransport {
        val attempts = mutableListOf<Attempt>()
        val active get() = attempts.filterNot { it.closed }
        var autoAuthenticate = true
        var onCleanup: suspend (Attempt) -> Unit = {}
        var read: suspend (Attempt) -> Unit = { it.finish.await()?.let { failure -> throw failure } }

        fun latest(endpoint: String = Endpoints.CONTROL) = attempts.last { it.endpoint == endpoint }

        fun count(endpoint: String) = attempts.count { it.endpoint == endpoint }

        override suspend fun run(
            target: Device.Server,
            setup: ClientConnectionSetup,
            endpointPath: String,
            onAuthenticated: () -> Unit,
        ) {
            val attempt = Attempt(target, endpointPath, onAuthenticated)
            attempts += attempt
            try {
                if (endpointPath == Endpoints.CONTROL && autoAuthenticate) onAuthenticated()
                read(attempt)
            } finally {
                withContext(NonCancellable) {
                    try {
                        onCleanup(attempt)
                    } finally {
                        attempt.closed = true
                    }
                }
            }
        }
    }

    class Link(
        val onStopped: () -> Unit,
        val fixture: ClientConnectionTestFixture,
    ) : BackgroundServiceLink {
        var starts = 0
        var stops = 0

        override fun start() {
            starts++
            fixture.startFailure?.let { throw it }
        }

        override fun stop() {
            stops++
            if (fixture.callbackOnRelease) onStopped()
        }
    }

    val target = Device.LocalServer(Uuid.random(), "Nursery", NetworkAddress("127.0.0.1"))
    val localDevice = Device.Client(Uuid.random(), "Monitor")
    val pairing = PairedDevice(target.id.toString(), "Camera", "fingerprint", "secret")
    val transport = RecordingTransport()
    val links = mutableListOf<Link>()
    val audio = MutableStateFlow(false)
    val video = MutableStateFlow(false)
    val relay = MutableStateFlow(RelayConfiguration("relay", "passphrase"))
    var relayFlow: Flow<RelayConfiguration> = relay
    var identityLoads = 0
    var loadIdentity: suspend () -> Device.Client = { localDevice }
    var findPairing: suspend () -> PairedDevice? = { pairing }
    var unpair: suspend () -> Unit = {}
    var unpairCalls = 0
    var startFailure: Exception? = null
    var callbackOnRelease = false
    val dispatcher = executionDispatcher
    val repository =
        DefaultNetworkClientRepository(
            networkControlDataSource = NetworkControlDataSource(),
            relayConfigurationRepository =
                object : RelayConfigurationRepository {
                    override val relayConfiguration get() = relayFlow
                },
            pairingStorageRepository =
                object : PairingStorageRepository {
                    override val pairedDevices = flowOf(emptyList<PairedDevice>())

                    override suspend fun find(deviceId: DeviceId) = findPairing()

                    override suspend fun pair(device: PairedDevice) = Unit

                    override suspend fun unpair(deviceId: DeviceId) {
                        unpairCalls++
                        unpair()
                    }

                    override suspend fun updateName(
                        deviceId: DeviceId,
                        newName: String,
                    ) = Unit
                },
            localClientDeviceRepository =
                object : LocalClientDeviceRepository {
                    override val localDevice =
                        flow {
                            identityLoads++
                            emit(loadIdentity())
                        }
                },
            coroutineContext = dispatcher + contextJob,
            foregroundContext = dispatcher,
            foregroundLinkFactory = { Link(it, this).also(links::add) },
            transport = transport,
            audioDemand = audio,
            videoDemand = video,
        )

    suspend fun connect() {
        repository.connect(target)
        scope.runCurrent()
    }

    suspend fun assertConnected() {
        assertIs<ConnectionState.Connected>(repository.connectionStateFlow.first())
    }

    suspend fun assertReason(reason: ErrorReason) {
        assertEquals(ConnectionState.Disconnected(reason), repository.connectionStateFlow.first())
    }

    suspend fun stop(reset: Boolean = false) {
        val stop = scope.launch { if (reset) repository.reset() else repository.disconnect() }
        scope.runCurrent()
        assertTrue(stop.isCompleted, "teardown must finish before stop returns")
        assertTrue(transport.active.isEmpty(), "no transport may survive teardown acknowledgement")
    }
}
