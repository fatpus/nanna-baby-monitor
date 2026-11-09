package org.vpilo.babymonitor.network.internal

/**
 * Tracks one Android foreground-service registration through injected registry operations, allowing JVM tests without starting a service.
 * [PlatformBackgroundServiceLink] creates a fresh instance for each start cycle.
 *
 * The registry can deliver stop callbacks during or after explicit unregistration. Marking release before calling [unregister]
 * suppresses those callbacks, while repeated start, stop and platform notifications are idempotent. Acquisition is recorded before
 * [register] runs because it can throw after adding the registry entry; the owner must still call [stop] to clean up.
 *
 * A platform stop can either clear the registry or leave the entry registered. [reportStopped] therefore only notifies the owner;
 * it retains the obligation to unregister on [stop]. The owner must arrange cleanup outside the notification callback.
 *
 * Registry operations and callback bookkeeping use separate locks so callbacks can arrive from another thread without acquiring
 * a lock held across a registry call.
 */
internal class BackgroundServiceRegistration(
    private val register: () -> Unit,
    private val unregister: () -> Unit,
    private val onStoppedByPlatform: () -> Unit,
) {
    // Registry calls may deliver callbacks on another thread. Never hold stateLock while entering the registry.
    private val operationLock = Any()
    private val stateLock = Any()
    private var startRequested = false
    private var releaseRequested = false
    private var stopReported = false

    fun start() {
        synchronized(operationLock) {
            synchronized(stateLock) {
                if (startRequested || releaseRequested) return
                // register() can acquire its registry entry before platform startup throws.
                startRequested = true
            }
            register()
        }
    }

    fun stop() {
        synchronized(operationLock) {
            synchronized(stateLock) {
                if (!startRequested || releaseRequested) return
                // Suppress callbacks before unregister(), including asynchronous notifications it schedules.
                releaseRequested = true
            }
            unregister()
        }
    }

    fun reportStopped() {
        synchronized(stateLock) {
            if (!startRequested || releaseRequested || stopReported) return
            stopReported = true
        }
        // Service shutdown may clear the registry or retain our entry. Only stop() releases the entry.
        onStoppedByPlatform()
    }
}
