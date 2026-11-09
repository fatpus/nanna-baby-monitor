package org.vpilo.babymonitor.network.model.usecase

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.network.model.ServerState
import org.vpilo.babymonitor.network.model.repository.NetworkClientRepository
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.uuid.Uuid

class IsConnectionAvailableFlowUseCaseTest {
    @Test
    fun onlyAnAuthenticatedConnectionIsAvailable() =
        runTest {
            val states = MutableStateFlow<ConnectionState>(ConnectionState.Disconnected(ConnectionState.ErrorReason.NotConnectedYet))
            val client =
                object : NetworkClientRepository {
                    override val connectionStateFlow = states
                    override val serverStateFlow = flowOf(ServerState())

                    override suspend fun connect(server: Device.Server) = Unit

                    override suspend fun disconnect() = Unit

                    override suspend fun reset() = Unit
                }
            val available = IsConnectionAvailableFlowUseCase(client)()
            val target = Device.LocalServer(Uuid.random(), "Camera", NetworkAddress("127.0.0.1"))
            for (state in listOf(
                ConnectionState.Disconnected(ConnectionState.ErrorReason.ClientQuit),
                ConnectionState.Connecting(target),
                ConnectionState.Connected(target),
                ConnectionState.Reconnecting(target),
            )) {
                states.value = state
                assertEquals(state is ConnectionState.Connected, available.first())
            }
        }
}
