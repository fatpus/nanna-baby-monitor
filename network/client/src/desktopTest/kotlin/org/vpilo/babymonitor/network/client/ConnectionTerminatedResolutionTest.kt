package org.vpilo.babymonitor.network.client

import org.vpilo.babymonitor.model.repository.ConnectionState.ErrorReason
import org.vpilo.babymonitor.network.model.transport.VersionMismatchException
import java.io.IOException
import java.net.ProtocolException
import java.security.cert.CertificateException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ConnectionTerminatedResolutionTest {
    @Test
    fun pairingRevocationCarriesItsReasonAndUnpairAction() {
        val failure = assertIs<ConnectionTerminatedException>(PairingRevokedException("No pairing"))
        assertEquals(ErrorReason.PairingRevoked, failure.reason)
        assertEquals(PostDisconnectionAction.UNPAIR, failure.action)
    }

    @Test
    fun certificateMismatchUnpairsTheTarget() {
        assertEquals(
            TerminationResolution(ErrorReason.CertificateMismatch, PostDisconnectionAction.UNPAIR),
            CertificateException("fingerprint mismatch").resolution,
        )
    }

    @Test
    fun versionMismatchResolvesToTheOutdatedSide() {
        assertEquals(
            TerminationResolution(ErrorReason.MonitorDeviceOutdated, PostDisconnectionAction.NONE),
            VersionMismatchException(1, 2, false).resolution,
        )
        assertEquals(
            TerminationResolution(ErrorReason.CameraDeviceOutdated, PostDisconnectionAction.NONE),
            VersionMismatchException(2, 1, false).resolution,
        )
        assertEquals(
            TerminationResolution(ErrorReason.AppRelayOutdated, PostDisconnectionAction.NONE),
            VersionMismatchException(1, 2, true).resolution,
        )
        assertEquals(
            TerminationResolution(ErrorReason.RelayOutdated, PostDisconnectionAction.NONE),
            VersionMismatchException(2, 1, true).resolution,
        )
    }

    @Test
    fun protocolClosureQuitsTheServerWithoutAction() {
        assertEquals(
            TerminationResolution(ErrorReason.ServerQuit, PostDisconnectionAction.NONE),
            ProtocolException("unsupported").resolution,
        )
    }

    @Test
    fun unrelatedFailuresResolveToServerNotFoundWithoutAction() {
        assertEquals(
            TerminationResolution(ErrorReason.ServerNotFound, PostDisconnectionAction.NONE),
            IOException("connection refused").resolution,
        )
    }
}
