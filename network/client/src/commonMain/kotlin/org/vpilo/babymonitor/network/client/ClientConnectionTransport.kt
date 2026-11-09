package org.vpilo.babymonitor.network.client

import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.network.client.websockets.audioStreamingClientWebSocket
import org.vpilo.babymonitor.network.client.websockets.controlClientWebSocket
import org.vpilo.babymonitor.network.client.websockets.videoStreamingClientWebSocket
import org.vpilo.babymonitor.network.internal.repository.InternalActiveSessionsRepository
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.RelayConfiguration
import org.vpilo.babymonitor.network.model.repository.PairingStorageRepository

internal data class ClientConnectionSetup(
    val localDevice: Device.Client,
    val expectedFingerprint: String,
    val relayConfiguration: RelayConfiguration,
)

internal interface ClientConnectionTransport {
    suspend fun run(
        target: Device.Server,
        setup: ClientConnectionSetup,
        endpointPath: String,
        onAuthenticated: () -> Unit,
    )
}

internal class DefaultClientConnectionTransport(
    private val networkControlDataSource: NetworkControlDataSource,
    private val networkAudioDataSource: NetworkAudioDataSource,
    private val networkVideoDataSource: NetworkVideoDataSource,
    private val pairingStorageRepository: PairingStorageRepository,
    private val activeSessionsRepository: InternalActiveSessionsRepository,
) : ClientConnectionTransport {
    override suspend fun run(
        target: Device.Server,
        setup: ClientConnectionSetup,
        endpointPath: String,
        onAuthenticated: () -> Unit,
    ) {
        WebSocketConnectionHandler(
            device = target,
            endpointPath = endpointPath,
            expectedFingerprint = setup.expectedFingerprint,
            relayConfiguration = setup.relayConfiguration,
            sessionBlock = {
                when (endpointPath) {
                    Endpoints.CONTROL -> {
                        controlClientWebSocket(
                            localDevice = setup.localDevice,
                            serverDeviceId = target.id,
                            dataSource = networkControlDataSource,
                            pairingStorageRepository = pairingStorageRepository,
                            activeSessionsRepository = activeSessionsRepository,
                            onConnected = onAuthenticated,
                        )
                    }

                    Endpoints.STREAM_AUDIO -> {
                        audioStreamingClientWebSocket(
                            localDevice = setup.localDevice,
                            serverDeviceId = target.id,
                            dataSource = networkAudioDataSource,
                            pairingStorageRepository = pairingStorageRepository,
                            activeSessionsRepository = activeSessionsRepository,
                        )
                    }

                    Endpoints.STREAM_VIDEO -> {
                        videoStreamingClientWebSocket(
                            localDevice = setup.localDevice,
                            serverDeviceId = target.id,
                            dataSource = networkVideoDataSource,
                            pairingStorageRepository = pairingStorageRepository,
                            activeSessionsRepository = activeSessionsRepository,
                        )
                    }

                    else -> {
                        error("Unknown endpoint: $endpointPath")
                    }
                }
            },
        ).run()
    }
}
