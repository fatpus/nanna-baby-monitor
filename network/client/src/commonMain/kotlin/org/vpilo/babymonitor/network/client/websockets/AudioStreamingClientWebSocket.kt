package org.vpilo.babymonitor.network.client.websockets

import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.EncodedAudioStreamChunk
import org.vpilo.babymonitor.model.repository.DeviceId
import org.vpilo.babymonitor.network.client.NetworkAudioDataSource
import org.vpilo.babymonitor.network.client.session.clientSessionHandshake
import org.vpilo.babymonitor.network.internal.repository.InternalActiveSessionsRepository
import org.vpilo.babymonitor.network.model.repository.PairingStorageRepository
import org.vpilo.babymonitor.network.security.protocol.StreamType
import org.vpilo.babymonitor.network.security.protocol.protocolReceiveAudio

internal suspend fun DefaultClientWebSocketSession.audioStreamingClientWebSocket(
    localDevice: Device.Client,
    serverDeviceId: DeviceId,
    dataSource: NetworkAudioDataSource,
    pairingStorageRepository: PairingStorageRepository,
    activeSessionsRepository: InternalActiveSessionsRepository,
) {
    val pairedDevice = pairingStorageRepository.find(serverDeviceId)
    val cipher = clientSessionHandshake(localDevice.id, serverDeviceId, pairedDevice, StreamType.AUDIO) ?: return

    activeSessionsRepository.register(serverDeviceId, this)
    try {
        while (true) {
            val frame: EncodedAudioStreamChunk = protocolReceiveAudio(cipher)
            dataSource.onChunkReceived(frame)
        }
    } finally {
        activeSessionsRepository.unregister(serverDeviceId, this)
    }
}
