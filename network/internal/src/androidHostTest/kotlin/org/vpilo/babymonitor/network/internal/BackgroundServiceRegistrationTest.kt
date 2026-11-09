package org.vpilo.babymonitor.network.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BackgroundServiceRegistrationTest {
    @Test
    fun explicitReleaseRemovesRegistrationOnce() {
        val registry = RegistrationFixture()
        registry.registration.start()

        registry.registration.stop()
        registry.registration.stop()

        assertFalse(registry.registered)
        assertEquals(1, registry.removalAttempts)
        assertEquals(1, registry.removals)
        assertEquals(0, registry.platformStops)
    }

    @Test
    fun callbackDuringReleaseDoesNotNotifyOrReleaseRecursively() {
        val registry = RegistrationFixture(notifyDuringRemoval = true)
        registry.registration.start()

        registry.registration.stop()

        assertFalse(registry.registered)
        assertEquals(1, registry.removalAttempts)
        assertEquals(0, registry.platformStops)
    }

    @Test
    fun platformStopWithClearedRegistryToleratesRemovalDuringCleanup() {
        val registry = RegistrationFixture()
        registry.registration.start()
        registry.registered = false

        registry.registration.reportStopped()

        assertEquals(1, registry.platformStops)
        assertEquals(0, registry.removalAttempts)
        registry.registration.stop()
        registry.registration.stop()
        assertEquals(1, registry.removalAttempts)
        assertEquals(0, registry.removals)
    }

    @Test
    fun platformStopWithRetainedRegistryDefersRemovalUntilCleanup() {
        val registry = RegistrationFixture(notifyDuringRemoval = true)
        registry.registration.start()

        registry.registration.reportStopped()

        assertTrue(registry.registered)
        assertEquals(1, registry.platformStops)
        assertEquals(0, registry.removalAttempts)
        registry.registration.stop()
        registry.registration.stop()
        assertFalse(registry.registered)
        assertEquals(1, registry.removalAttempts)
        assertEquals(1, registry.removals)
        assertEquals(1, registry.platformStops)
    }

    @Test
    fun delayedNotificationAfterReleaseIsSuppressed() {
        val registry = RegistrationFixture()
        registry.registration.start()
        registry.registration.stop()

        registry.registration.reportStopped()

        assertEquals(0, registry.platformStops)
        assertEquals(1, registry.removalAttempts)
    }

    @Test
    fun registrationThrowingAfterAcquisitionStillReleasesOnce() {
        val failure = IllegalStateException("foreground start denied")
        val registry = RegistrationFixture(registrationFailure = failure, notifyDuringRemoval = true)

        assertEquals(failure, assertFailsWith<IllegalStateException> { registry.registration.start() })
        assertTrue(registry.registered)
        registry.registration.stop()
        registry.registration.stop()

        assertFalse(registry.registered)
        assertEquals(1, registry.removalAttempts)
        assertEquals(1, registry.removals)
        assertEquals(0, registry.platformStops)
    }

    @Test
    fun repeatedStartDoesNotAcquireTwice() {
        val registry = RegistrationFixture()

        registry.registration.start()
        registry.registration.start()
        registry.registration.stop()

        assertEquals(1, registry.acquisitions)
        assertEquals(1, registry.removals)
    }

    @Test
    fun repeatedPlatformNotificationOnlyNotifiesOnce() {
        val registry = RegistrationFixture()
        registry.registration.start()

        registry.registration.reportStopped()
        registry.registration.reportStopped()
        registry.registration.stop()

        assertEquals(1, registry.platformStops)
        assertEquals(1, registry.removals)
    }

    private class RegistrationFixture(
        registrationFailure: Throwable? = null,
        notifyDuringRemoval: Boolean = false,
    ) {
        var registered = false
        var acquisitions = 0
        var removalAttempts = 0
        var removals = 0
        var platformStops = 0
        private var removing = false

        val registration: BackgroundServiceRegistration =
            BackgroundServiceRegistration(
                register = {
                    registered = true
                    acquisitions++
                    registrationFailure?.let { throw it }
                },
                unregister = {
                    assertFalse(removing, "Unregistration must not recursively enter itself")
                    removing = true
                    try {
                        removalAttempts++
                        if (registered) {
                            registered = false
                            removals++
                        }
                        if (notifyDuringRemoval) reportStopped()
                    } finally {
                        removing = false
                    }
                },
                onStoppedByPlatform = { platformStops++ },
            )

        private fun reportStopped(): Unit = registration.reportStopped()
    }
}
