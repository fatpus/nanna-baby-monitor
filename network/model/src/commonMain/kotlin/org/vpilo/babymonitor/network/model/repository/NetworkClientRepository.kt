package org.vpilo.babymonitor.network.model.repository

import kotlinx.coroutines.flow.Flow
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.network.model.ServerState

interface NetworkClientRepository {
    val connectionStateFlow: Flow<ConnectionState>

    val serverStateFlow: Flow<ServerState>

    /** Returns when the processor accepts ownership, before setup or authentication. */
    suspend fun connect(server: Device.Server)

    /** Returns after all preceding connection-owned network work has finished cleanup. */
    suspend fun disconnect()

    /** Completes the same teardown as disconnect, restoring the initial disconnected state. */
    suspend fun reset()
}
