package org.vpilo.babymonitor.network.internal

import android.content.Context
import androidx.lifecycle.LifecycleOwner
import org.vpilo.babymonitor.android.service.AndroidService
import org.vpilo.babymonitor.android.service.AndroidServiceRegistry
import org.vpilo.babymonitor.model.AppRole

/**
 * Holds an Android foreground-service registration while network components need to run in the background.
 *
 * [AndroidServiceRegistry.unregister] can schedule an asynchronous [AndroidService.onServiceStopped] callback even for an explicit
 * [stop]. Each start creates its own service and [BackgroundServiceRegistration], which suppresses explicit-release notifications
 * and prevents delayed callbacks from an earlier registration being treated as a platform stop for a later start.
 *
 * A platform-initiated stop notifies the owner through [onStoppedByPlatform]. The owner must arrange teardown outside that callback
 * and call [stop] to release any retained registry entry. The registration is also retained if [start] throws after partial acquisition,
 * so the same cleanup path can release it.
 */
internal actual class PlatformBackgroundServiceLink actual constructor(
    private val role: AppRole,
    private val onStoppedByPlatform: () -> Unit,
) : BackgroundServiceLink {
    private val operationLock = Any()
    private var registration: BackgroundServiceRegistration? = null

    actual override fun start() {
        synchronized(operationLock) {
            if (registration != null) return
            val nextRegistration = createRegistration()
            // Retain this registration for cleanup even when foreground startup fails after acquisition.
            registration = nextRegistration
            nextRegistration.start()
        }
    }

    actual override fun stop() {
        synchronized(operationLock) {
            registration?.stop()
            registration = null
        }
    }

    private fun createRegistration(): BackgroundServiceRegistration {
        lateinit var nextRegistration: BackgroundServiceRegistration
        val service =
            object : AndroidService {
                override val role: AppRole = this@PlatformBackgroundServiceLink.role

                override fun onServiceStarted(
                    context: Context,
                    lifecycleOwner: LifecycleOwner,
                ) = Unit

                // Capture this start cycle, so a late callback cannot address a later registration.
                override fun onServiceStopped() = nextRegistration.reportStopped()
            }
        nextRegistration =
            BackgroundServiceRegistration(
                register = { AndroidServiceRegistry.register(service) },
                unregister = { AndroidServiceRegistry.unregister(service) },
                onStoppedByPlatform = onStoppedByPlatform,
            )
        return nextRegistration
    }
}
