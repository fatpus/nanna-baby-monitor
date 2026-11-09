package org.vpilo.babymonitor.network.internal

import org.vpilo.babymonitor.model.AppRole

/**
 * Desktop [BackgroundServiceLink] to keep components alive in the background.
 * This does nothing, as on Desktop there are no restrictions to keep the app running in the background.
 */
internal actual class PlatformBackgroundServiceLink actual constructor(
    role: AppRole,
    onStoppedByPlatform: () -> Unit,
) : BackgroundServiceLink {
    actual override fun start() = Unit

    actual override fun stop() = Unit
}
