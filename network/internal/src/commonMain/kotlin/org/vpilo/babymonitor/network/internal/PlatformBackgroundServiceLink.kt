package org.vpilo.babymonitor.network.internal

import org.vpilo.babymonitor.model.AppRole

/** Platform implementation hidden behind the public [BackgroundServiceLink] factory and contract. */
internal expect class PlatformBackgroundServiceLink(
    role: AppRole,
    onStoppedByPlatform: () -> Unit,
) : BackgroundServiceLink {
    override fun start()

    override fun stop()
}
