package org.vpilo.babymonitor.network.client

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.wss
import io.ktor.http.HttpMethod
import io.ktor.websocket.CloseReason
import io.ktor.websocket.close
import io.ktor.websocket.pingInterval
import io.ktor.websocket.timeout
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import kotlinx.coroutines.withContext
import org.vpilo.babymonitor.common.Logger
import org.vpilo.babymonitor.model.AppRole
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.network.internal.protocol.asVersionMismatchExceptionOrNull
import org.vpilo.babymonitor.network.model.Constants
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.RelayConfiguration
import org.vpilo.babymonitor.network.model.transport.VersionMismatchException
import org.vpilo.babymonitor.network.security.crypto.PinnedTrustManager
import org.vpilo.babymonitor.network.security.relay.relayWss
import java.net.ProtocolException
import java.security.cert.CertificateException
import kotlin.coroutines.cancellation.CancellationException
import io.ktor.client.plugins.websocket.pingInterval as clientPingInterval

internal class WebSocketConnectionHandler(
    private val device: Device,
    private val endpointPath: String,
    private val sessionBlock: suspend DefaultClientWebSocketSession.() -> Unit,
    private val expectedFingerprint: String,
    private val relayConfiguration: RelayConfiguration? = null,
) {
    /** Owns one connection and finishes all acquired transport resources before returning. */
    suspend fun run() {
        Logger.i(TAG) { "Connecting to $device for $endpointPath" }
        try {
            if (device is Device.RemoteServer) {
                handleRelaySession()
            } else {
                startWebSocket(device.address)
            }
        } catch (ex: CancellationException) {
            throw ex
        } catch (
            @Suppress("TooGenericExceptionCaught") ex: Exception,
        ) {
            // CIO and structured child failures can wrap security/protocol failures.
            throw (ex.terminalConnectionFailureOrNull() ?: ex)
        }
    }

    private suspend fun startWebSocket(host: NetworkAddress) {
        val client =
            HttpClient(CIO) {
                install(WebSockets) { clientPingInterval = Constants.WEBSOCKET_PING_PERIOD }
                engine {
                    https {
                        trustManager = PinnedTrustManager(expectedFingerprint)
                        serverName = Constants.TLS_SERVER_NAME
                    }
                }
            }
        try {
            client.wss(
                method = HttpMethod.Get,
                host = host.address,
                port = Constants.SERVICE_PORT,
                path = endpointPath,
            ) {
                pingInterval = Constants.WEBSOCKET_PING_PERIOD
                timeout = Constants.WEBSOCKET_TIMEOUT
                runSession()
            }
        } finally {
            client.close()
            // CIO may retain a TLS producer after failed negotiation. These jobs belong
            // exclusively to this connection, so stop remaining children before joining.
            client.coroutineContext.cancel()
            client.engine.coroutineContext.cancel()
            withContext(NonCancellable) {
                client.coroutineContext.job.join()
                client.engine.coroutineContext.job
                    .join()
            }
        }
    }

    private suspend fun handleRelaySession() {
        val configuration = checkNotNull(relayConfiguration) { "Relay configuration must be provided for remote servers" }
        check(configuration.isConfigured) { "No relay configured - cannot connect to $device" }
        relayWss(
            configuration = configuration,
            role = AppRole.CLIENT,
            endpoint = relayEndpointFor(endpointPath),
            serverId = device.id,
        ) {
            runSession()
        }
    }

    private suspend fun DefaultClientWebSocketSession.runSession() {
        // Ktor runs the reader in the calling coroutine. The session owns a separate job;
        // capture that receiver explicitly, before entering the cleanup context.
        val sessionJob = this.coroutineContext.job
        try {
            var readerFailure: Throwable? = null
            try {
                sessionBlock()
            } catch (ex: CancellationException) {
                throw ex
            } catch (
                @Suppress("TooGenericExceptionCaught") ex: Exception,
            ) {
                readerFailure = ex
            }

            // A reader or structured child can finish while the peer is still healthy.
            // Initiate local closure before awaiting the close reason, never the reverse.
            close()
            val reason = closeReason.await()
            val originalTerminalFailure = readerFailure?.terminalConnectionFailureOrNull()
            val failure =
                reason?.asVersionMismatchExceptionOrNull()
                    ?: originalTerminalFailure?.takeUnless { it is ProtocolException }
                    ?: when (reason?.knownReason) {
                        CloseReason.Codes.VIOLATED_POLICY -> PairingRevokedException(reason.message)
                        CloseReason.Codes.PROTOCOL_ERROR -> ProtocolException(reason.message)
                        else -> originalTerminalFailure ?: readerFailure
                    }
            if (failure != null) throw failure
        } finally {
            this.cancel()
            withContext(NonCancellable) { sessionJob.join() }
        }
    }

    private companion object {
        private const val TAG = "WebSocketConnectionHandler"

        private fun relayEndpointFor(endpointPath: String): String =
            when (endpointPath) {
                Endpoints.CONTROL -> Endpoints.Relay.CLIENT_CONTROL
                Endpoints.STREAM_AUDIO -> Endpoints.Relay.CLIENT_AUDIO
                Endpoints.STREAM_VIDEO -> Endpoints.Relay.CLIENT_VIDEO
                else -> error("Unknown endpoint: $endpointPath")
            }
    }
}

/** Finds a domain failure even when structured cancellation and socket teardown wrap it. */
internal fun Throwable.terminalConnectionFailureOrNull(): Throwable? {
    val pending = ArrayDeque<Throwable>()
    val visited = mutableSetOf<Throwable>()
    val failures = mutableListOf<Throwable>()
    pending.add(this)
    while (pending.isNotEmpty()) {
        val failure = pending.removeFirst()
        if (!visited.add(failure)) continue
        failures.add(failure)
        failure.cause?.let(pending::addLast)
        failure.suppressedExceptions.forEach(pending::addLast)
    }
    return failures.firstOrNull { it is VersionMismatchException }
        ?: failures.firstOrNull { it is CertificateException || it is PairingRevokedException }
        ?: failures.firstOrNull { it is ProtocolException }
}
