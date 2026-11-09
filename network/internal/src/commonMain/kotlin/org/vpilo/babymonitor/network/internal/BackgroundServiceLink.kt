package org.vpilo.babymonitor.network.internal

import org.vpilo.babymonitor.model.AppRole

/**
 * Links network service lifetimes to platform background-service permissions, allowing networking to continue while the app is in
 * the background. Create the platform implementation with `BackgroundServiceLink(role, onStoppedByPlatform)`.
 * Owners can inject implementations of this contract in tests; the platform implementation itself is internal and final.
 */
interface BackgroundServiceLink {
    /**
     * Starts the link to keep the server alive in the background.
     * Must be called from a context where the platform allows starting the foreground affordance.
     */
    fun start()

    /**
     * Releases the background service registration. Safe to call repeatedly or after platform shutdown/startup failure.
     */
    fun stop()
}

/**
 * Creates a platform link for [role]. [onStoppedByPlatform] must only request cleanup without waiting for it.
 * Explicit [BackgroundServiceLink.stop] calls suppress their own platform notifications.
 */
fun BackgroundServiceLink(
    role: AppRole,
    onStoppedByPlatform: () -> Unit,
): BackgroundServiceLink = PlatformBackgroundServiceLink(role, onStoppedByPlatform)
