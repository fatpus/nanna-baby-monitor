package org.vpilo.babymonitor.network.di

import kotlinx.coroutines.Dispatchers
import org.koin.core.module.Module
import org.koin.core.module.dsl.factoryOf
import org.koin.core.module.dsl.singleOf
import org.koin.dsl.bind
import org.koin.dsl.module
import org.vpilo.babymonitor.model.AppRole
import org.vpilo.babymonitor.model.repository.StreamingAudioReceiverRepository
import org.vpilo.babymonitor.model.repository.StreamingVideoReceiverRepository
import org.vpilo.babymonitor.network.client.ClientConnectionTransport
import org.vpilo.babymonitor.network.client.DefaultClientConnectionTransport
import org.vpilo.babymonitor.network.client.DefaultNetworkClientRepository
import org.vpilo.babymonitor.network.client.NetworkAudioDataSource
import org.vpilo.babymonitor.network.client.NetworkAudioReceiverRepository
import org.vpilo.babymonitor.network.client.NetworkControlDataSource
import org.vpilo.babymonitor.network.client.NetworkVideoDataSource
import org.vpilo.babymonitor.network.client.NetworkVideoReceiverRepository
import org.vpilo.babymonitor.network.client.discovery.DefaultRemoteDiscoveryRepository
import org.vpilo.babymonitor.network.client.pairing.DefaultClientPairingRepository
import org.vpilo.babymonitor.network.internal.BackgroundServiceLink
import org.vpilo.babymonitor.network.internal.di.networkInternalKoinModule
import org.vpilo.babymonitor.network.model.pairing.ClientPairingRepository
import org.vpilo.babymonitor.network.model.repository.NetworkClientRepository
import org.vpilo.babymonitor.network.model.repository.RemoteDiscoveryRepository
import org.vpilo.babymonitor.network.security.di.networkSecurityKoinModule
import org.vpilo.babymonitor.network.security.identity.DeviceIdentity
import kotlin.coroutines.CoroutineContext

val networkClientKoinModule: Module =
    module {
        includes(networkInternalKoinModule)
        includes(networkSecurityKoinModule)

        singleOf(::NetworkControlDataSource)
        singleOf(::NetworkAudioDataSource)
        singleOf(::NetworkVideoDataSource)

        // SharedResourceHolder repositories must be singletons to maintain their state.
        singleOf(::NetworkAudioReceiverRepository)
            .bind<StreamingAudioReceiverRepository>()
        singleOf(::NetworkVideoReceiverRepository)
            .bind<StreamingVideoReceiverRepository>()

        factoryOf(::DefaultClientConnectionTransport)
            .bind<ClientConnectionTransport>()

        single<NetworkClientRepository> {
            DefaultNetworkClientRepository(
                networkControlDataSource = get(),
                relayConfigurationRepository = get(),
                pairingStorageRepository = get(),
                localClientDeviceRepository = get(),
                transport = get(),
                audioDemand = get<StreamingAudioReceiverRepository>().isActive,
                videoDemand = get<StreamingVideoReceiverRepository>().isActive,
                coroutineContext = get<CoroutineContext>(),
                foregroundContext = Dispatchers.Main.immediate,
                foregroundLinkFactory = { onStopped -> BackgroundServiceLink(AppRole.CLIENT, onStopped) },
            )
        }

        singleOf(::DefaultRemoteDiscoveryRepository)
            .bind<RemoteDiscoveryRepository>()
        single { DefaultClientPairingRepository(loadIdentity = { DeviceIdentity.loadOrCreate() }) }
            .bind<ClientPairingRepository>()
    }
