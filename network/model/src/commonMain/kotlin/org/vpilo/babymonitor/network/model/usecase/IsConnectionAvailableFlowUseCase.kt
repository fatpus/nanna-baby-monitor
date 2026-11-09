package org.vpilo.babymonitor.network.model.usecase

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.network.model.repository.NetworkClientRepository

class IsConnectionAvailableFlowUseCase(
    private val networkClientRepository: NetworkClientRepository,
) {
    operator fun invoke(): Flow<Boolean> = networkClientRepository.connectionStateFlow.map { it is ConnectionState.Connected }
}
