package org.vpilo.babymonitor.network.client

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.vpilo.babymonitor.codec.VideoDecoder
import org.vpilo.babymonitor.model.OpaqueVideoStream
import org.vpilo.babymonitor.model.repository.StreamingVideoReceiverRepository
import kotlin.coroutines.CoroutineContext

internal class NetworkVideoReceiverRepository(
    dataSource: NetworkVideoDataSource,
    coroutineContext: CoroutineContext,
) : StreamingVideoReceiverRepository {
    private val coroutineScope = CoroutineScope(coroutineContext)
    private val decoder = VideoDecoder(input = dataSource.frames, coroutineContext = coroutineContext)
    override val videoStream: OpaqueVideoStream = decoder.videoStream
    override val isActive: Flow<Boolean> = videoStream.isActive

    init {
        isActive
            .onEach { active ->
                if (active) decoder.start() else decoder.stop()
            }.launchIn(coroutineScope)
    }
}
