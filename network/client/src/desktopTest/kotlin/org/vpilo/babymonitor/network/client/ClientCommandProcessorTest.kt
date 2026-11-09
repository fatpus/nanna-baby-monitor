package org.vpilo.babymonitor.network.client

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.network.model.Endpoints
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.uuid.Uuid

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ClientCommandProcessorTest {
    @Test
    fun processorDoesNotStartBeforeTheFirstCommand() =
        runTest {
            val dispatcher = RecordingDispatcher(StandardTestDispatcher(testScheduler))
            val fixture = ClientConnectionTestFixture(this, executionDispatcher = dispatcher)
            assertTrue(dispatcher.jobs.isEmpty(), "constructing the singleton must not schedule a processor")
            runCurrent()
            assertTrue(dispatcher.jobs.isEmpty())
            fixture.stop(reset = true)
            assertTrue(dispatcher.jobs.all { it.isCompleted })
        }

    @Test
    fun resetCompletesAllOwnedJobsAndTheNextCommandStartsAFreshProcessor() =
        runTest {
            val dispatcher = RecordingDispatcher(StandardTestDispatcher(testScheduler))
            val fixture = ClientConnectionTestFixture(this, executionDispatcher = dispatcher)
            fixture.connect()
            val oldJobs = dispatcher.jobs.toSet()
            assertTrue(oldJobs.any { it.isActive })
            fixture.stop(reset = true)
            assertTrue(oldJobs.all { it.isCompleted }, "reset must complete the processor and its supervisor as well as the connection")
            assertTrue(dispatcher.jobs.all { it.isCompleted })
            fixture.connect()
            fixture.assertConnected()
            assertEquals(2, fixture.transport.count(Endpoints.CONTROL))
            assertTrue((dispatcher.jobs - oldJobs).any { it.isActive })
            assertTrue(oldJobs.all { it.isCompleted })
            fixture.stop(reset = true)
            assertTrue(dispatcher.jobs.all { it.isCompleted })
        }

    @Test
    fun acceptedResetFinishesProcessorShutdownEvenIfItsCallerIsCancelled() =
        runTest {
            val dispatcher = RecordingDispatcher(StandardTestDispatcher(testScheduler))
            val fixture = ClientConnectionTestFixture(this, executionDispatcher = dispatcher)
            fixture.connect()
            val cleanup = CompletableDeferred<Unit>()
            fixture.transport.onCleanup = { cleanup.await() }
            val reset = launch { fixture.repository.reset() }
            runCurrent()
            assertFalse(reset.isCompleted)
            reset.cancel()
            cleanup.complete(Unit)
            runCurrent()
            fixture.assertReason(ErrorReason.NotConnectedYet)
            assertTrue(fixture.transport.active.isEmpty())
            assertTrue(dispatcher.jobs.all { it.isCompleted })
        }

    @Test
    fun connectQueuedDuringResetWaitsForThePreviousProcessorToFinish() =
        runTest {
            val dispatcher = RecordingDispatcher(StandardTestDispatcher(testScheduler))
            val fixture = ClientConnectionTestFixture(this, executionDispatcher = dispatcher)
            fixture.connect()
            val cleanup = CompletableDeferred<Unit>()
            fixture.transport.onCleanup = { cleanup.await() }
            val reset = launch { fixture.repository.reset() }
            runCurrent()
            val oldJobs = dispatcher.jobs.toSet()
            val next = Device.LocalServer(Uuid.random(), "Next", NetworkAddress("10.0.0.2"))
            val connect = launch(start = CoroutineStart.UNDISPATCHED) { fixture.repository.connect(next) }
            runCurrent()
            assertFalse(reset.isCompleted)
            assertFalse(connect.isCompleted)
            assertEquals(1, fixture.identityLoads)
            cleanup.complete(Unit)
            runCurrent()
            assertTrue(reset.isCompleted)
            assertTrue(connect.isCompleted)
            assertTrue(oldJobs.all { it.isCompleted })
            assertEquals(ConnectionState.Connected(next), fixture.repository.connectionStateFlow.first())
            fixture.stop(reset = true)
            assertTrue(dispatcher.jobs.all { it.isCompleted })
        }

    @Test
    fun platformCallbacksCannotRestartAProcessorAfterReset() =
        runTest {
            val dispatcher = RecordingDispatcher(StandardTestDispatcher(testScheduler))
            val fixture = ClientConnectionTestFixture(this, executionDispatcher = dispatcher)
            fixture.callbackOnRelease = true
            fixture.connect()
            val oldLink = fixture.links.single()
            fixture.stop(reset = true)
            val completedJobs = dispatcher.jobs.toSet()
            assertTrue(completedJobs.all { it.isCompleted })
            oldLink.onStopped()
            runCurrent()
            assertEquals(completedJobs, dispatcher.jobs)
            assertTrue(dispatcher.jobs.all { it.isCompleted })
            fixture.connect()
            oldLink.onStopped()
            runCurrent()
            fixture.assertConnected()
            fixture.stop(reset = true)
            assertTrue(dispatcher.jobs.all { it.isCompleted })
        }

    @Test
    fun resetCancelledBeforeAcceptanceDoesNotStopTheReplacement() =
        runTest {
            val fixture = ClientConnectionTestFixture(this)
            fixture.connect()
            val cleanup = CompletableDeferred<Unit>()
            val first = fixture.transport.latest()
            fixture.transport.onCleanup = { if (it === first) cleanup.await() }
            val next = Device.LocalServer(Uuid.random(), "Next", NetworkAddress("10.0.0.2"))
            fixture.repository.connect(next)
            runCurrent()
            val reset = launch(start = CoroutineStart.UNDISPATCHED) { fixture.repository.reset() }
            reset.cancel()
            cleanup.complete(Unit)
            runCurrent()
            assertEquals(ConnectionState.Connected(next), fixture.repository.connectionStateFlow.first())
            fixture.stop(reset = true)
        }

    private class RecordingDispatcher(
        private val delegate: CoroutineDispatcher,
    ) : CoroutineDispatcher() {
        val jobs = mutableSetOf<Job>()

        override fun dispatch(
            context: CoroutineContext,
            block: Runnable,
        ) {
            var job = context[Job]
            while (job != null && job !== NonCancellable) {
                jobs += job
                job = job.parent
            }
            delegate.dispatch(context, block)
        }
    }
}
