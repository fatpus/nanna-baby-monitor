package org.vpilo.babymonitor.network.client

import io.ktor.client.plugins.ResponseException
import io.ktor.client.plugins.websocket.WebSocketException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.vpilo.babymonitor.common.Logger
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.model.repository.LocalClientDeviceRepository
import org.vpilo.babymonitor.network.internal.BackgroundServiceLink
import org.vpilo.babymonitor.network.model.Constants
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.ServerState
import org.vpilo.babymonitor.network.model.repository.NetworkClientRepository
import org.vpilo.babymonitor.network.model.repository.PairingStorageRepository
import org.vpilo.babymonitor.network.model.repository.RelayConfigurationRepository
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume

@Suppress("LongParameterList")
internal class DefaultNetworkClientRepository(
    networkControlDataSource: NetworkControlDataSource,
    private val relayConfigurationRepository: RelayConfigurationRepository,
    private val pairingStorageRepository: PairingStorageRepository,
    private val localClientDeviceRepository: LocalClientDeviceRepository,
    private val transport: ClientConnectionTransport,
    private val audioDemand: Flow<Boolean>,
    private val videoDemand: Flow<Boolean>,
    coroutineContext: CoroutineContext,
    private val foregroundContext: CoroutineContext,
    private val foregroundLinkFactory: (onStopped: () -> Unit) -> BackgroundServiceLink,
) : NetworkClientRepository {
    private val commandContext = coroutineContext.minusKey(Job)
    private val commandLock = Any()
    private var commandProcessor: Job? = null
    private val commands = Channel<Command>(Channel.UNLIMITED)
    private val connectionState =
        MutableStateFlow<ConnectionState>(ConnectionState.Disconnected(ErrorReason.NotConnectedYet))
    override val connectionStateFlow: Flow<ConnectionState> = connectionState.asStateFlow()
    override val serverStateFlow: Flow<ServerState> = networkControlDataSource.serverState

    /** Creates one processor on demand; reset completes its owner before another processor can start. */
    private fun createCommandProcessor(firstCommand: Command): Job {
        val owner = SupervisorJob()
        val processorScope = CoroutineScope(commandContext + owner)
        var resetFinished: CompletableDeferred<Unit>? = null
        val processor =
            processorScope.launch(start = CoroutineStart.LAZY) {
                try {
                    resetFinished = processCommands(processorScope, owner, firstCommand)
                } finally {
                    owner.complete()
                }
            }
        commandProcessor = owner
        owner.invokeOnCompletion {
            val next =
                synchronized(commandLock) {
                    commandProcessor = null
                    // Platform notifications still queued here belong to the processor that just stopped.
                    var pending = commands.tryReceive().getOrNull()
                    while (pending is Command.PlatformStop || pending?.accepted?.isCancelled == true) {
                        pending = commands.tryReceive().getOrNull()
                    }
                    pending?.let(::createCommandProcessor)
                }
            // Complete reset only after both the processor and all its connection children have finished.
            resetFinished?.complete(Unit)
            next?.start()
        }
        return processor
    }

    private suspend fun processCommands(
        processorScope: CoroutineScope,
        owner: Job,
        firstCommand: Command,
    ): CompletableDeferred<Unit> {
        var current: Connection? = null
        var command = firstCommand
        try {
            while (true) {
                if (!command.accepted.complete(Unit)) {
                    command = commands.receive()
                    continue
                }
                val result = command.process(current, processorScope, owner)
                current = result.connection
                current?.job?.start()
                result.resetFinished?.let { return it }
                command = commands.receive()
            }
        } finally {
            withContext(NonCancellable) { current?.job?.cancelAndJoin() }
        }
    }

    private suspend fun Command.process(
        current: Connection?,
        processorScope: CoroutineScope,
        owner: Job,
    ): CommandResult =
        when (this) {
            is Command.Connect -> {
                if (current?.job?.isActive == true && current.target.sameEndpoint(target)) {
                    CommandResult(current)
                } else {
                    current?.job?.cancelAndJoin()
                    val next = Connection(target)
                    next.job =
                        processorScope.launch(start = CoroutineStart.LAZY) {
                            runConnection(next) { reportPlatformStop(owner, next) }
                        }
                    CommandResult(next)
                }
            }

            is Command.Stop -> {
                current?.job?.cancelAndJoin()
                connectionState.value = ConnectionState.Disconnected(reason)
                if (reason == ErrorReason.NotConnectedYet) {
                    CommandResult(null, finished)
                } else {
                    finished.complete(Unit)
                    CommandResult(null)
                }
            }

            is Command.PlatformStop -> {
                if (current === connection && current.job.isActive) {
                    current.job.cancelAndJoin()
                    connectionState.value = ConnectionState.Disconnected(ErrorReason.ClientQuit)
                    CommandResult(null)
                } else {
                    CommandResult(current)
                }
            }
        }

    private fun enqueueCommand(command: Command) {
        val processor =
            synchronized(commandLock) {
                if (commandProcessor == null) {
                    createCommandProcessor(command)
                } else {
                    commands.trySend(command).getOrThrow()
                    null
                }
            }
        processor?.start()
    }

    private fun reportPlatformStop(
        owner: Job,
        connection: Connection,
    ) {
        synchronized(commandLock) {
            if (commandProcessor === owner && owner.isActive) {
                commands.trySend(Command.PlatformStop(connection))
            }
        }
    }

    override suspend fun connect(server: Device.Server) = sendCommand(Command.Connect(server))

    override suspend fun disconnect() = stop(ErrorReason.ClientQuit)

    override suspend fun reset() = stop(ErrorReason.NotConnectedYet)

    private suspend fun stop(reason: ErrorReason) {
        val command = Command.Stop(reason)
        sendCommand(command)
        command.finished.await()
    }

    /** Cancellation and processor acceptance race on the same deferred, without parenting work to the caller. */
    private suspend fun sendCommand(command: Command) =
        suspendCancellableCoroutine { continuation ->
            continuation.invokeOnCancellation { command.accepted.cancel() }
            command.accepted.invokeOnCompletion { failure ->
                if (failure == null) continuation.resume(Unit) else continuation.cancel(failure)
            }
            if (continuation.isActive) enqueueCommand(command)
        }

    private suspend fun runConnection(
        connection: Connection,
        onStopped: () -> Unit,
    ) {
        val target = connection.target
        var link: BackgroundServiceLink? = null
        var reason = ErrorReason.ServerNotFound
        try {
            connectionState.value = ConnectionState.Connecting(target)
            withContext(foregroundContext) {
                link = foregroundLinkFactory(onStopped)
                link.start()
            }
            coroutineScope {
                // Observe the snapshot before other suspended loading, so later setup cannot miss a change.
                val relay = relayConfigurationRepository.relayConfiguration.first()
                val watcher =
                    if (target is Device.RemoteServer) {
                        launch(start = CoroutineStart.UNDISPATCHED) {
                            relayConfigurationRepository.relayConfiguration.collect { configuration ->
                                if (configuration != relay) throw RelayConfigurationChanged()
                            }
                        }
                    } else {
                        null
                    }
                try {
                    val localDevice = localClientDeviceRepository.localDevice.first()
                    val fingerprint =
                        pairingStorageRepository.find(target.id)?.certFingerprint
                            ?: throw PairingRevokedException("No pairing for $target")
                    currentCoroutineContext().ensureActive()
                    runControl(target, ClientConnectionSetup(localDevice, fingerprint, relay))
                } finally {
                    watcher?.cancel()
                }
            }
        } catch (ex: CancellationException) {
            throw ex
        } catch (ex: RelayConfigurationChanged) {
            reason = ErrorReason.ClientQuit
            Logger.w(TAG) { "Connection to $target ended: $ex" }
        } catch (
            @Suppress("TooGenericExceptionCaught") ex: Exception,
        ) {
            val resolution =
                ex.terminalConnectionFailureOrNull()?.resolution
                    ?: TerminationResolution(ErrorReason.ServerNotFound, PostDisconnectionAction.NONE)
            reason = resolution.reason
            Logger.w(TAG) { "Connection to $target ended: $ex" }
            if (resolution.action == PostDisconnectionAction.UNPAIR) {
                withContext(NonCancellable) {
                    try {
                        pairingStorageRepository.unpair(target.id)
                    } catch (
                        @Suppress("TooGenericExceptionCaught") storageFailure: Exception,
                    ) {
                        Logger.e(TAG) { "Could not persist unpairing of $target: $storageFailure" }
                    }
                }
            }
        } finally {
            withContext(NonCancellable + foregroundContext) {
                try {
                    link?.stop()
                } catch (
                    @Suppress("TooGenericExceptionCaught") ex: Exception,
                ) {
                    Logger.e(TAG) { "Could not release foreground registration: $ex" }
                }
            }
        }
        currentCoroutineContext().ensureActive()
        connectionState.value = ConnectionState.Disconnected(reason)
    }

    private suspend fun runControl(
        target: Device.Server,
        setup: ClientConnectionSetup,
    ) {
        var retriesRemaining = 0
        runWithConnectionRetries(
            target,
            "Control",
            beforeRetry = {
                if (retriesRemaining == 0) throw IOException("Control connection unavailable")
                retriesRemaining--
                connectionState.value = ConnectionState.Reconnecting(target)
            },
        ) {
            runControlAttempt(target, setup) {
                retriesRemaining = MAX_RECONNECTION_ATTEMPTS
                connectionState.value = ConnectionState.Connected(target)
            }
        }
    }

    private suspend fun runControlAttempt(
        target: Device.Server,
        setup: ClientConnectionSetup,
        onAuthenticated: () -> Unit,
    ) = coroutineScope {
        val media = mutableListOf<Job>()
        try {
            transport.run(target, setup, Endpoints.CONTROL) {
                coroutineContext.ensureActive()
                onAuthenticated()
                if (media.isEmpty()) {
                    media += launch { observeMedia(audioDemand, target, setup, Endpoints.STREAM_AUDIO) }
                    media += launch { observeMedia(videoDemand, target, setup, Endpoints.STREAM_VIDEO) }
                }
            }
        } finally {
            // A normal reader return must cancel infinite demand observers as well as exceptional returns.
            media.forEach { it.cancel() }
            withContext(NonCancellable) { media.joinAll() }
        }
    }

    private suspend fun observeMedia(
        demand: Flow<Boolean>,
        target: Device.Server,
        setup: ClientConnectionSetup,
        endpoint: String,
    ) {
        demand.distinctUntilChanged().collectLatest { active ->
            if (active) {
                runWithConnectionRetries(target, endpoint) {
                    transport.run(target, setup, endpoint) {}
                }
            }
        }
    }

    private suspend fun runWithConnectionRetries(
        target: Device.Server,
        connectionName: String,
        beforeRetry: () -> Unit = {},
        runAttempt: suspend () -> Unit,
    ) {
        flow<Nothing> {
            currentCoroutineContext().ensureActive()
            runAttempt()
            // A normal socket close should follow the same retry policy as a recoverable failure.
            throw ConnectionAttemptCompleted()
        }.retryWhen { failure, _ ->
            if (failure !is Exception) return@retryWhen false
            if (failure is CancellationException) currentCoroutineContext().ensureActive()
            failure.terminalConnectionFailureOrNull()?.let { throw it }
            if (failure !is ConnectionAttemptCompleted) {
                if (!failure.isRecoverable()) return@retryWhen false
                Logger.i(TAG) { "$connectionName connection to $target lost: $failure" }
            }
            beforeRetry()
            delay(Constants.RECONNECTION_TIMEOUT)
            true
        }.collect()
    }

    private class CommandResult(
        val connection: Connection?,
        val resetFinished: CompletableDeferred<Unit>? = null,
    )

    private class Connection(
        val target: Device.Server,
    ) {
        lateinit var job: Job
    }

    private sealed class Command {
        val accepted = CompletableDeferred<Unit>()

        class Connect(
            val target: Device.Server,
        ) : Command()

        class Stop(
            val reason: ErrorReason,
        ) : Command() {
            val finished = CompletableDeferred<Unit>()
        }

        class PlatformStop(
            val connection: Connection,
        ) : Command()
    }

    private class ConnectionAttemptCompleted : Exception()

    private class RelayConfigurationChanged : Exception()

    private companion object {
        private const val TAG = "DefaultNetworkClientRepository"
        private const val MAX_RECONNECTION_ATTEMPTS = 3

        private fun Device.Server.sameEndpoint(other: Device.Server): Boolean =
            id == other.id && (this is Device.RemoteServer) == (other is Device.RemoteServer) && address == other.address

        fun Throwable.isRecoverable(): Boolean =
            this is IOException || this is ResponseException || this is WebSocketException || this is ClosedReceiveChannelException
    }
}
