package org.vpilo.babymonitor.app.menu

import androidx.compose.runtime.Stable
import kotlinx.coroutines.launch
import org.vpilo.babymonitor.errorreport.model.ErrorReportingRepository
import org.vpilo.babymonitor.model.repository.AppRoleRepository
import org.vpilo.babymonitor.model.viewmodel.AppViewModel
import org.vpilo.babymonitor.network.model.repository.NetworkClientRepository
import org.vpilo.babymonitor.network.model.usecase.IsConnectionAvailableFlowUseCase

@Stable
class MenuScreenViewModel(
    private val appRoleRepository: AppRoleRepository,
    private val networkClientRepository: NetworkClientRepository,
    private val errorReportingRepository: ErrorReportingRepository,
    private val isConnectionAvailableFlowUseCase: IsConnectionAvailableFlowUseCase,
) : AppViewModel<MenuScreenAction, MenuScreenState, MenuScreenEffect>(
        TAG = "MenuScreenViewModel",
        initialState = MenuScreenState(),
    ) {
    override fun SubscriptionScope.onSubscribed() {
        appRoleRepository.appRole.subscribe {
            state.copy(currentRole = it).update()
        }
        isConnectionAvailableFlowUseCase().subscribe {
            state.copy(isConnected = it).update()
        }
    }

    override fun onAction(action: MenuScreenAction) {
        when (action) {
            MenuScreenAction.Disconnect -> {
                vmScope.launch {
                    networkClientRepository.disconnect()
                    MenuScreenEffect.Disconnected.sendEffect()
                }
            }

            MenuScreenAction.SendErrorReport -> {
                vmScope.launch {
                    errorReportingRepository.makeManualReport()
                }
            }
        }
    }
}
