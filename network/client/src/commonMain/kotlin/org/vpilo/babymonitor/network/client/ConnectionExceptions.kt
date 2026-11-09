package org.vpilo.babymonitor.network.client

import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.network.model.transport.VersionMismatch
import org.vpilo.babymonitor.network.model.transport.VersionMismatchException
import java.net.ProtocolException
import java.security.cert.CertificateException

/**
 * A follow-up action that [ConnectionTerminatedException] may request.
 */
internal enum class PostDisconnectionAction {
    NONE,
    UNPAIR,
}

/**
 * A failure that ends the connection lifetime without retrying to connect.
 */
internal open class ConnectionTerminatedException(
    val reason: ErrorReason,
    val action: PostDisconnectionAction,
    message: String? = null,
    cause: Throwable? = null,
) : Exception(message, cause)

/**
 * Thrown when the server closes a connection because this client is no longer paired with it.
 */
internal class PairingRevokedException(
    message: String,
) : ConnectionTerminatedException(ErrorReason.PairingRevoked, PostDisconnectionAction.UNPAIR, message)

/**
 * The error reason a terminal failure maps to, and the action it requests.
 */
internal data class TerminationResolution(
    val reason: ErrorReason,
    val action: PostDisconnectionAction,
)

/**
 * Resolves a [ConnectionTerminatedException] connection failure to its error reason and requested action.
 *
 * Termination failures outside this hierarchy (java security and protocol types, [VersionMismatchException]) cannot extend
 * [ConnectionTerminatedException] across module boundaries, so they are mapped here.
 */
internal val Throwable.resolution: TerminationResolution
    get() =
        when (this) {
            is ConnectionTerminatedException -> {
                TerminationResolution(reason, action)
            }

            is VersionMismatchException if isRelayConnection -> {
                TerminationResolution(
                    if (mismatch == VersionMismatch.LOCAL_OUTDATED) ErrorReason.AppRelayOutdated else ErrorReason.RelayOutdated,
                    PostDisconnectionAction.NONE,
                )
            }

            is VersionMismatchException -> {
                TerminationResolution(
                    if (mismatch == VersionMismatch.LOCAL_OUTDATED) ErrorReason.MonitorDeviceOutdated else ErrorReason.CameraDeviceOutdated,
                    PostDisconnectionAction.NONE,
                )
            }

            is CertificateException -> {
                TerminationResolution(ErrorReason.CertificateMismatch, PostDisconnectionAction.UNPAIR)
            }

            is ProtocolException -> {
                TerminationResolution(ErrorReason.ServerQuit, PostDisconnectionAction.NONE)
            }

            else -> {
                TerminationResolution(ErrorReason.ServerNotFound, PostDisconnectionAction.NONE)
            }
        }
