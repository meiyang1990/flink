/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 【学习笔记】PartitionRequestQueue - 分区请求出站队列
 *
 * <p>核心职责：
 * 1. 管理所有分区数据读取器（NetworkSequenceViewReader），协调数据发送
 * 2. 监听 Channel 可写性变化，在可写时触发数据发送
 * 3. 处理 Credit 通告和消费恢复请求，与下游协调流量控制
 *
 * <p>工作机制：
 * - 当 Reader 有数据可读时，会被加入 availableReaders 队列
 * - writeAndFlushNextMessageIfPossible 方法轮询队列，发送数据
 * - 使用 Netty 的 Channel.isWritable() 实现背压控制
 *
 * <p>与其他组件的关系：
 * - PartitionRequestServerHandler：接收请求后创建 Reader 并注册到此队列
 * - NetworkSequenceViewReader：从 ResultSubpartition 读取数据
 * - NettyServer：通过 Netty Pipeline 连接，处理网络 I/O
 *
 * <p>线程模型：
 * - 所有操作都在 Netty I/O 线程中执行
 * - 通过 userEventTriggered 机制实现线程安全的队列操作
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

    // 写入完成监听器
    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    // 可用读取器队列：已有数据可发送的读取器
    private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

    // 所有读取器映射：按 InputChannelID 索引
    private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders =
            new ConcurrentHashMap<>();

    // 致命错误标记
    private boolean fatalError;

    // Netty Channel 上下文
    private ChannelHandlerContext ctx;

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelRegistered(ctx);
    }

    // 通知读取器有数据可读，触发入队操作
    void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
        // 通过事件循环调度，确保线程安全
        ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
    }

    /**
     * 尝试将读取器加入可用队列。
     * 当收到 Credit 通告或数据可用通知时调用。
     */
    private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
        // 避免重复入队
        if (reader.isRegisteredAsAvailable()) {
            return;
        }

        ResultSubpartitionView.AvailabilityWithBacklog availabilityWithBacklog =
                reader.getAvailabilityAndBacklog();
        if (!availabilityWithBacklog.isAvailable()) {
            // 数据不可用但有积压，通告下游
            int backlog = availabilityWithBacklog.getBacklog();
            if (backlog > 0 && reader.needAnnounceBacklog()) {
                announceBacklog(reader, backlog);
            }
            return;
        }

        // 队列为空时触发写入
        boolean triggerWrite = availableReaders.isEmpty();
        registerAvailableReader(reader);

        if (triggerWrite) {
            writeAndFlushNextMessageIfPossible(ctx.channel());
        }
    }

    /**
     * Accesses internal state to verify reader registration in the unit tests.
     *
     * <p><strong>Do not use anywhere else!</strong>
     *
     * @return readers which are enqueued available for transferring data
     */
    @VisibleForTesting
    ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
        return availableReaders;
    }

    public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
        allReaders.put(reader.getReceiverId(), reader);
    }

    public void cancel(InputChannelID receiverId) {
        ctx.pipeline().fireUserEventTriggered(receiverId);
    }

    public void close() throws IOException {
        if (ctx != null) {
            ctx.channel().close();
        }

        releaseAllResources();
    }

    /**
     * Adds unannounced credits from the consumer or resumes data consumption after an exactly-once
     * checkpoint and enqueues the corresponding reader for this consumer (if not enqueued yet).
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param operation The operation to be performed (add credit or resume data consumption).
     */
    void addCreditOrResumeConsumption(
            InputChannelID receiverId, Consumer<NetworkSequenceViewReader> operation)
            throws Exception {
        if (fatalError) {
            return;
        }

        NetworkSequenceViewReader reader = obtainReader(receiverId);

        operation.accept(reader);
        enqueueAvailableReader(reader);
    }

    void acknowledgeAllRecordsProcessed(InputChannelID receiverId) {
        if (fatalError) {
            return;
        }

        obtainReader(receiverId).acknowledgeAllRecordsProcessed();
    }

    void notifyNewBufferSize(InputChannelID receiverId, int newBufferSize) {
        if (fatalError) {
            return;
        }

        // It is possible to receive new buffer size before the reader would be created since the
        // downstream task could calculate buffer size even using the data from one channel but it
        // sends new buffer size into all upstream even if they don't ready yet. In this case, just
        // ignore the new buffer size.
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyNewBufferSize(newBufferSize);
        }
    }

    /**
     * Notify the id of required segment from the consumer.
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    void notifyRequiredSegmentId(InputChannelID receiverId, int subpartitionId, int segmentId) {
        if (fatalError) {
            return;
        }
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyRequiredSegmentId(subpartitionId, segmentId);
        }
    }

    NetworkSequenceViewReader obtainReader(InputChannelID receiverId) {
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader == null) {
            throw new IllegalStateException(
                    "No reader for receiverId = " + receiverId + " exists.");
        }

        return reader;
    }

    /**
     * Announces remaining backlog to the consumer after the available data notification or data
     * consumption resumption.
     */
    private void announceBacklog(NetworkSequenceViewReader reader, int backlog) {
        checkArgument(backlog > 0, "Backlog must be positive.");

        NettyMessage.BacklogAnnouncement announcement =
                new NettyMessage.BacklogAnnouncement(backlog, reader.getReceiverId());
        ctx.channel()
                .writeAndFlush(announcement)
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        onChannelFutureFailure(future);
                                    }
                                });
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // The user event triggered event loop callback is used for thread-safe
        // hand over of reader queues and cancelled producers.

        if (msg instanceof NetworkSequenceViewReader) {
            enqueueAvailableReader((NetworkSequenceViewReader) msg);
        } else if (msg.getClass() == InputChannelID.class) {
            // Release partition view that get a cancel request.
            InputChannelID toCancel = (InputChannelID) msg;

            // remove reader from queue of available readers
            availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

            // remove reader from queue of all readers and release its resource
            final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
            if (toRelease != null) {
                releaseViewReader(toRelease);
            }
        } else if (msg instanceof PartitionRequestListener) {
            PartitionRequestListener partitionRequestListener = (PartitionRequestListener) msg;

            // Send partition not found message to the downstream task when the listener is timeout.
            final ResultPartitionID resultPartitionId =
                    partitionRequestListener.getResultPartitionId();
            final InputChannelID inputChannelId = partitionRequestListener.getReceiverId();
            availableReaders.remove(partitionRequestListener.getViewReader());
            allReaders.remove(inputChannelId);
            try {
                ctx.writeAndFlush(
                        new NettyMessage.ErrorResponse(
                                new PartitionNotFoundException(resultPartitionId), inputChannelId));
            } catch (Exception e) {
                LOG.warn(
                        "Write partition not found exception to {} for result partition {} fail",
                        inputChannelId,
                        resultPartitionId,
                        e);
            }
        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    // Channel 可写性变化时触发
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    /**
     * 核心发送逻辑：从可用队列中取出读取器，读取数据并发送。
     * 类似于 InputGate 消费本地 InputChannel 的逻辑。
     */
    private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
        // 致命错误或 Channel 不可写时跳过
        if (fatalError || !channel.isWritable()) {
            return;
        }

        BufferAndAvailability next = null;
        int nextSubpartitionId = -1;
        try {
            while (true) {
                NetworkSequenceViewReader reader = pollAvailableReader();

                // 队列为空，等待下次触发
                if (reader == null) {
                    return;
                }

                nextSubpartitionId = reader.peekNextBufferSubpartitionId();
                next = reader.getNextBuffer();
                if (next == null) {
                    if (!reader.isReleased()) {
                        continue;
                    }

                    // 读取器已释放，发送错误响应
                    Throwable cause = reader.getFailureCause();
                    if (cause != null) {
                        ErrorResponse msg = new ErrorResponse(cause, reader.getReceiverId());
                        ctx.writeAndFlush(msg);
                    }
                } else {
                    // 如果还有更多数据，将读取器重新入队
                    if (next.moreAvailable()) {
                        registerAvailableReader(reader);
                    }

                    // 构建并发送 BufferResponse
                    BufferResponse msg =
                            new BufferResponse(
                                    next.buffer(),
                                    next.getSequenceNumber(),
                                    reader.getReceiverId(),
                                    nextSubpartitionId,
                                    next.buffer() instanceof FullyFilledBuffer
                                            ? ((FullyFilledBuffer) next.buffer())
                                                    .getPartialBuffers()
                                                    .size()
                                            : 0,
                                    next.buffersInBacklog());

                    // 写入并等待完成后继续处理下一个
                    channel.writeAndFlush(msg).addListener(writeListener);

                    return;
                }
            }
        } catch (Throwable t) {
            if (next != null) {
                next.buffer().recycleBuffer();
            }

            throw new IOException(t.getMessage(), t);
        }
    }

    private void registerAvailableReader(NetworkSequenceViewReader reader) {
        availableReaders.add(reader);
        reader.setRegisteredAsAvailable(true);
    }

    @Nullable
    private NetworkSequenceViewReader pollAvailableReader() {
        NetworkSequenceViewReader reader = availableReaders.poll();
        if (reader != null) {
            reader.setRegisteredAsAvailable(false);
        }
        return reader;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseAllResources();

        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        handleException(ctx.channel(), cause);
    }

    private void handleException(Channel channel, Throwable cause) throws IOException {
        LOG.error(
                "Encountered error while consuming partitions (connection to {})",
                channel.remoteAddress(),
                cause);

        fatalError = true;
        releaseAllResources();

        if (channel.isActive()) {
            channel.writeAndFlush(new ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void releaseAllResources() throws IOException {
        // note: this is only ever executed by one thread: the Netty IO thread!
        for (NetworkSequenceViewReader reader : allReaders.values()) {
            releaseViewReader(reader);
        }

        availableReaders.clear();
        allReaders.clear();
    }

    private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
        reader.setRegisteredAsAvailable(false);
        reader.releaseAllResources();
    }

    private void onChannelFutureFailure(ChannelFuture future) throws Exception {
        if (future.cause() != null) {
            handleException(future.channel(), future.cause());
        } else {
            handleException(
                    future.channel(), new IllegalStateException("Sending cancelled by user."));
        }
    }

    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        ctx.pipeline().fireUserEventTriggered(partitionRequestListener);
    }

    // This listener is called after an element of the current nonEmptyReader has been
    // flushed. If successful, the listener triggers further processing of the
    // queues.
    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else {
                    onChannelFutureFailure(future);
                }
            } catch (Throwable t) {
                handleException(future.channel(), t);
            }
        }
    }
}
