package org.vpilo.babymonitor.network.client

import io.ktor.http.HttpStatusCode
import io.ktor.network.tls.certificates.buildKeyStore
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
import io.ktor.server.response.respond
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.WebSocketSession
import io.ktor.websocket.close
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.vpilo.babymonitor.model.AppRole
import org.vpilo.babymonitor.model.Device
import org.vpilo.babymonitor.model.NetworkAddress
import org.vpilo.babymonitor.model.repository.ConnectionState
import org.vpilo.babymonitor.model.repository.DeviceId
import org.vpilo.babymonitor.model.repository.LocalClientDeviceRepository
import org.vpilo.babymonitor.network.internal.BackgroundServiceLink
import org.vpilo.babymonitor.network.internal.repository.InternalActiveSessionsRepository
import org.vpilo.babymonitor.network.model.Constants
import org.vpilo.babymonitor.network.model.Endpoints
import org.vpilo.babymonitor.network.model.RelayConfiguration
import org.vpilo.babymonitor.network.model.ServerState
import org.vpilo.babymonitor.network.model.pairing.PairedDevice
import org.vpilo.babymonitor.network.model.repository.PairingStorageRepository
import org.vpilo.babymonitor.network.model.repository.RelayConfigurationRepository
import org.vpilo.babymonitor.network.security.crypto.computeServerHandshakeProof
import org.vpilo.babymonitor.network.security.crypto.deriveServerSessionCipher
import org.vpilo.babymonitor.network.security.crypto.generateSessionSalt
import org.vpilo.babymonitor.network.security.crypto.sha256Fingerprint
import org.vpilo.babymonitor.network.security.crypto.verifyClientHandshakeProof
import org.vpilo.babymonitor.network.security.protocol.StreamType
import org.vpilo.babymonitor.network.security.protocol.receiveSessionHandshakeRequestOrNull
import org.vpilo.babymonitor.network.security.protocol.sendServerMessage
import org.vpilo.babymonitor.network.security.protocol.sendSessionHandshakeResponse
import java.security.cert.X509Certificate
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.encoding.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class ReconnectionTest {
    @Test
    fun aDroppedConnectionRestoresAnAuthenticatedSession() =
        runBlocking {
            withCamera {
                connect()
                dropConnection()
                awaitConnection(2)
            }
        }

    @Test
    fun reconnectionSurvivesTwoUnsuccessfulAttempts() =
        runBlocking {
            withCamera {
                connect()
                rejectedAttempts.set(2)
                dropConnection()
                awaitConnection(4)
                assertEquals(4, attempts.get())
            }
        }

    @Test
    fun aSuccessfulReconnectionRestoresTheRetryBudgetForTheNextDrop() =
        runBlocking {
            withCamera {
                connect()
                rejectedAttempts.set(2)
                dropConnection()
                awaitConnection(4)
                rejectedAttempts.set(2)
                dropConnection()
                awaitConnection(7)
            }
        }

    @Test
    fun revokedPairingStopsReconnection() =
        runBlocking {
            withCamera {
                connect()
                revokePairing()
                val disconnected =
                    withTimeout(5.seconds) {
                        repository.connectionStateFlow.first { it is ConnectionState.Disconnected }
                    }
                assertEquals(ConnectionState.Disconnected(ConnectionState.ErrorReason.PairingRevoked), disconnected)
                delay(3_500.milliseconds)
                assertEquals(1, attempts.get())
            }
        }

    @Test
    fun anUnavailableCameraIsRetriedThreeTimesBeforeGivingUp() =
        runBlocking {
            withCamera {
                connect()
                rejectedAttempts.set(10)
                dropConnection()
                val disconnected =
                    withTimeout(20.seconds) {
                        repository.connectionStateFlow.first { it is ConnectionState.Disconnected }
                    }
                assertEquals(ConnectionState.Disconnected(ConnectionState.ErrorReason.ServerNotFound), disconnected)
                assertEquals(4, attempts.get())
                assertEquals(0, activeSessions.get())
            }
        }

    @Test
    fun manuallyDisconnectingCancelsPendingReconnection() =
        runBlocking {
            withCamera {
                connect()
                dropConnection()
                repository.disconnect()
                delay(3_500.milliseconds)
                assertEquals(1, attempts.get())
                assertEquals(
                    ConnectionState.Disconnected(ConnectionState.ErrorReason.ClientQuit),
                    repository.connectionStateFlow.first(),
                )
            }
        }

    private suspend fun withCamera(test: suspend Camera.() -> Unit) {
        val camera = Camera()
        try {
            withTimeout(25.seconds) { camera.test() }
        } finally {
            camera.repository.disconnect()
            camera.server.stop(0, 0)
        }
    }

    private class Camera {
        val device = Device.LocalServer(Uuid.random(), "Nursery", NetworkAddress("127.0.0.1"))
        val attempts = AtomicInteger()
        val rejectedAttempts = AtomicInteger()
        val activeSessions = AtomicInteger()
        private val sessions = Channel<DefaultWebSocketServerSession>(Channel.UNLIMITED)
        private val sharedSecret = ByteArray(32) { it.toByte() }
        private val keyStore =
            buildKeyStore {
                certificate("camera") {
                    password = "test-password"
                    domains = listOf(Constants.TLS_SERVER_NAME, "127.0.0.1")
                    keySizeInBits = 2048
                    daysValid = 1
                }
            }
        private val pairedDevices =
            MutableStateFlow(
                listOf(
                    PairedDevice(
                        device.id.toString(),
                        device.name,
                        (keyStore.getCertificate("camera") as X509Certificate).sha256Fingerprint(),
                        Base64.encode(sharedSecret),
                    ),
                ),
            )
        private val pairingStorage =
            object : PairingStorageRepository {
                override val pairedDevices = this@Camera.pairedDevices

                override suspend fun find(deviceId: DeviceId) = pairedDevices.value.firstOrNull()

                override suspend fun pair(device: PairedDevice) = error("Unexpected pairing")

                override suspend fun unpair(deviceId: DeviceId) {
                    pairedDevices.value = emptyList()
                }

                override suspend fun updateName(
                    deviceId: DeviceId,
                    newName: String,
                ) = error("Unexpected rename")
            }
        private val ledger =
            object : InternalActiveSessionsRepository {
                override fun register(
                    clientId: DeviceId,
                    session: WebSocketSession,
                ) {
                    activeSessions.incrementAndGet()
                }

                override fun unregister(
                    clientId: DeviceId,
                    session: WebSocketSession,
                ) {
                    activeSessions.decrementAndGet()
                }

                override suspend fun closeSessions(
                    clientId: DeviceId,
                    wasUnpaired: Boolean,
                ) = Unit
            }
        private val controlDataSource = NetworkControlDataSource()
        val repository =
            DefaultNetworkClientRepository(
                networkControlDataSource = controlDataSource,
                relayConfigurationRepository =
                    object : RelayConfigurationRepository {
                        override val relayConfiguration = flowOf(RelayConfiguration(host = "", passphrase = ""))
                    },
                pairingStorageRepository = pairingStorage,
                localClientDeviceRepository =
                    object : LocalClientDeviceRepository {
                        override val localDevice = flowOf(Device.Client(Uuid.random(), "Monitor"))
                    },
                coroutineContext = Dispatchers.Default,
                foregroundContext = Dispatchers.Default,
                foregroundLinkFactory = { BackgroundServiceLink(AppRole.CLIENT, it) },
                transport =
                    DefaultClientConnectionTransport(
                        controlDataSource,
                        NetworkAudioDataSource(),
                        NetworkVideoDataSource(),
                        pairingStorage,
                        ledger,
                    ),
                audioDemand = MutableStateFlow(false),
                videoDemand = MutableStateFlow(false),
            )
        val server: EmbeddedServer<*, *> =
            embeddedServer(
                factory = Netty,
                configure = {
                    sslConnector(
                        keyStore = keyStore,
                        keyAlias = "camera",
                        keyStorePassword = { "test-password".toCharArray() },
                        privateKeyPassword = { "test-password".toCharArray() },
                    ) {
                        host = "127.0.0.1"
                        port = Constants.SERVICE_PORT
                    }
                },
                module = {
                    install(WebSockets)
                    intercept(ApplicationCallPipeline.Call) {
                        attempts.incrementAndGet()
                        if (rejectedAttempts.getAndUpdate { (it - 1).coerceAtLeast(0) } > 0) {
                            call.respond(HttpStatusCode.ServiceUnavailable)
                            finish()
                        }
                    }
                    routing {
                        webSocket(Endpoints.CONTROL) {
                            incoming.receive() // Device protocol version.
                            val request = assertNotNull(receiveSessionHandshakeRequestOrNull())
                            assertTrue(verifyClientHandshakeProof(sharedSecret, request.salt, request.proof))
                            val salt = generateSessionSalt()
                            sendSessionHandshakeResponse(salt, computeServerHandshakeProof(sharedSecret, request.salt, salt))
                            val cipher =
                                deriveServerSessionCipher(
                                    sharedSecret,
                                    request.salt,
                                    salt,
                                    device.id.toByteArray() + byteArrayOf(StreamType.CONTROL.tag),
                                )
                            sendServerMessage(ServerState(batteryLevel = attempts.get()), cipher)
                            sessions.send(this)
                            incoming.consumeEach {
                                // Keep the authenticated session open until the test closes it.
                            }
                        }
                    }
                },
            ).start(wait = false)

        suspend fun connect() {
            repository.connect(device)
            awaitConnection(1)
        }

        suspend fun awaitConnection(attempt: Int) {
            withTimeout(15.seconds) {
                repository.serverStateFlow.first { it.batteryLevel == attempt }
                assertEquals(ConnectionState.Connected(device), repository.connectionStateFlow.first())
            }
        }

        suspend fun dropConnection() {
            sessions.receive().close(CloseReason(CloseReason.Codes.GOING_AWAY, "Connection lost"))
            withTimeout(5.seconds) {
                repository.connectionStateFlow.first { it is ConnectionState.Reconnecting }
            }
        }

        suspend fun revokePairing() {
            sessions.receive().close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "Unpaired"))
        }
    }
}
