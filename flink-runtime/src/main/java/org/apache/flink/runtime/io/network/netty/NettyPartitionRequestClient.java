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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 远程分区请求客户端 - Netty 实现的 PartitionRequestClient。
 *
 * <p>核心职责：
 * <ul>
 *   <li>封装到远程 TaskManager 的 TCP 连接（Netty Channel）</li>
 *   <li>发送分区请求（PartitionRequest）建立数据通道</li>
 *   <li>发送 Credit 更新（AddCredit）进行流量控制</li>
 *   <li>发送 TaskEvent（如 Checkpoint Barrier 确认）</li>
 *   <li>管理连接生命周期（引用计数、复用、关闭）</li>
 * </ul>
 *
 * <p>连接复用机制：
 * <ul>
 *   <li>多个 RemoteInputChannel 到同一远程 TaskManager 共享同一个 Client</li>
 *   <li>通过 closeReferenceCounter 引用计数管理：每个 InputChannel 注册时 +1，关闭时 -1</li>
 *   <li>当计数归零且不允许复用时，关闭底层 TCP 连接</li>
 * </ul>
 *
 * <p>消息类型：
 * <ul>
 *   <li>PartitionRequest：请求指定分区的数据，携带初始 Credit</li>
 *   <li>AddCredit：增加可用 Credit（允许服务端发送更多 Buffer）</li>
 *   <li>TaskEventRequest：反向发送 Task 事件到上游生产者</li>
 *   <li>ResumeConsumption：从 Checkpoint 阻塞中恢复消费</li>
 *   <li>NewBufferSize：通知生产者新的 Buffer 大小</li>
 *   <li>CloseRequest：请求关闭连接</li>
 * </ul>
 *
 * <p>异常处理：
 * <ul>
 *   <li>发送失败时通过 ChannelFutureListener 回调 InputChannel.onError()</li>
 *   <li>同时发送 ConnectionErrorMessage 通知对端连接出错</li>
 * </ul>
 *
 * Partition request client for remote partition requests.
 *
 * <p>This client is shared by all remote input channels, which request a partition from the same
 * {@link ConnectionID}.
 */
public class NettyPartitionRequestClient implements PartitionRequestClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyPartitionRequestClient.class);

    /** 底层 Netty TCP Channel，与远程 TaskManager 的网络连接 */
    private final Channel tcpChannel;

    /**
     * 客户端响应处理器，处理来自服务端的 BufferResponse、ErrorResponse 等。
     * 内部维护 InputChannelId -> RemoteInputChannel 的映射。
     */
    private final NetworkClientHandler clientHandler;

    /** 连接标识，包含远程地址、ResourceID 和连接索引 */
    private final ConnectionID connectionId;

    /** 请求客户端工厂，用于管理客户端生命周期和连接复用 */
    private final PartitionRequestClientFactory clientFactory;

    /**
     * 关闭引用计数：跟踪有多少 InputChannel 正在使用此客户端。
     * 当计数为 0 时，TCP 连接可以安全关闭。
     * If zero, the underlying TCP channel can be safely closed.
     */
    private final AtomicInteger closeReferenceCounter = new AtomicInteger(0);

    /** 标记客户端是否已关闭，避免重复关闭 */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    NettyPartitionRequestClient(
            Channel tcpChannel,
            NetworkClientHandler clientHandler,
            ConnectionID connectionId,
            PartitionRequestClientFactory clientFactory) {

        this.tcpChannel = checkNotNull(tcpChannel);
        this.clientHandler = checkNotNull(clientHandler);
        this.connectionId = checkNotNull(connectionId);
        this.clientFactory = checkNotNull(clientFactory);
        clientHandler.setConnectionId(connectionId);
    }

    boolean canBeDisposed() {
        return closeReferenceCounter.get() == 0 && !canBeReused();
    }

    /**
     * Validate the client and increment the reference counter.
     *
     * <p>Note: the reference counter has to be incremented before returning the instance of this
     * client to ensure correct closing logic.
     *
     * @return whether this client can be used.
     */
    boolean validateClientAndIncrementReferenceCounter() {
        if (!clientHandler.hasChannelError()) {
            return closeReferenceCounter.incrementAndGet() > 0;
        }
        return false;
    }

    /**
     * Requests a remote intermediate result partition queue.
     *
     * <p>The request goes to the remote producer, for which this partition request client instance
     * has been created.
     */
    @Override
    public void requestSubpartition(
            final ResultPartitionID partitionId,
            final ResultSubpartitionIndexSet subpartitionIndexSet,
            final RemoteInputChannel inputChannel,
            int delayMs)
            throws IOException {

        checkNotClosed();

        LOG.debug(
                "Requesting subpartition {} of partition {} with {} ms delay.",
                subpartitionIndexSet,
                partitionId,
                delayMs);

        // 将 InputChannel 注册到 clientHandler，用于接收响应时路由到正确的 Channel
        clientHandler.addInputChannel(inputChannel);

        // 构建分区请求消息，携带分区 ID、子分区索引集合、接收者 ID 和初始 Credit
        final PartitionRequest request =
                new PartitionRequest(
                        partitionId,
                        subpartitionIndexSet,
                        inputChannel.getInputChannelId(),
                        inputChannel.getInitialCredit());

        // 发送失败时的回调处理：移除 InputChannel、通知错误、发送连接错误消息
        final ChannelFutureListener listener =
                future -> {
                    if (!future.isSuccess()) {
                        // 发送失败：从 handler 移除 channel，通知 InputChannel 错误
                        clientHandler.removeInputChannel(inputChannel);
                        inputChannel.onError(
                                new LocalTransportException(
                                        String.format(
                                                "Sending the partition request to '%s [%s] (#%d)' failed.",
                                                connectionId.getAddress(),
                                                connectionId
                                                        .getResourceID()
                                                        .getStringWithMetadata(),
                                                connectionId.getConnectionIndex()),
                                        future.channel().localAddress(),
                                        future.cause()));
                        // 发送连接错误消息到 pipeline，触发错误处理流程
                        sendToChannel(
                                new ConnectionErrorMessage(
                                        future.cause() == null
                                                ? new RuntimeException(
                                                        "Cannot send partition request.")
                                                : future.cause()));
                    }
                };

        // 根据延迟配置决定立即发送还是延迟发送
        if (delayMs == 0) {
            // 立即发送请求
            ChannelFuture f = tcpChannel.writeAndFlush(request);
            f.addListener(listener);
        } else {
            // 延迟发送：用于失败重试时的退避策略
            final ChannelFuture[] f = new ChannelFuture[1];
            tcpChannel
                    .eventLoop()
                    .schedule(
                            () -> {
                                f[0] = tcpChannel.writeAndFlush(request);
                                f[0].addListener(listener);
                            },
                            delayMs,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Sends a task event backwards to an intermediate result partition producer.
     *
     * <p>Backwards task events flow between readers and writers and therefore will only work when
     * both are running at the same time, which is only guaranteed to be the case when both the
     * respective producer and consumer task run pipelined.
     */
    @Override
    public void sendTaskEvent(
            ResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel)
            throws IOException {
        checkNotClosed();

        tcpChannel
                .writeAndFlush(
                        new TaskEventRequest(event, partitionId, inputChannel.getInputChannelId()))
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        inputChannel.onError(
                                                new LocalTransportException(
                                                        String.format(
                                                                "Sending the task event to '%s [%s] (#%d)' failed.",
                                                                connectionId.getAddress(),
                                                                connectionId
                                                                        .getResourceID()
                                                                        .getStringWithMetadata(),
                                                                connectionId.getConnectionIndex()),
                                                        future.channel().localAddress(),
                                                        future.cause()));
                                        sendToChannel(
                                                new ConnectionErrorMessage(
                                                        future.cause() == null
                                                                ? new RuntimeException(
                                                                        "Cannot send task event.")
                                                                : future.cause()));
                                    }
                                });
    }

    /**
     * 通知服务端有新的 Credit 可用。
     * Credit-based 流量控制的核心：下游有新的缓冲区可用时，通过此方法通知上游。
     */
    @Override
    public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
        sendToChannel(new AddCreditMessage(inputChannel));
    }

    /** 通知服务端新的 Buffer 大小配置 */
    @Override
    public void notifyNewBufferSize(RemoteInputChannel inputChannel, int bufferSize) {
        sendToChannel(new NewBufferSizeMessage(inputChannel, bufferSize));
    }

    /** 通知服务端需要的 Segment ID（用于 Tiered Storage） */
    @Override
    public void notifyRequiredSegmentId(
            RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {
        sendToChannel(new SegmentIdMessage(inputChannel, subpartitionIndex, segmentId));
    }

    /**
     * 恢复消费：从 Checkpoint 阻塞中恢复。
     * 在对齐 Checkpoint 时，遇到 Barrier 后会暂停该 Channel 的消费，
     * 收到所有上游的 Barrier 后调用此方法恢复。
     */
    @Override
    public void resumeConsumption(RemoteInputChannel inputChannel) {
        sendToChannel(new ResumeConsumptionMessage(inputChannel));
    }

    /** 确认所有用户记录已处理（用于 EndOfData 事件同步） */
    @Override
    public void acknowledgeAllRecordsProcessed(RemoteInputChannel inputChannel) {
        sendToChannel(new AcknowledgeAllRecordsProcessedMessage(inputChannel));
    }

    /**
     * 发送消息到 Channel Pipeline。
     * 通过 fireUserEventTriggered 触发用户事件，由 Handler 处理实际发送。
     */
    private void sendToChannel(Object message) {
        tcpChannel.eventLoop().execute(() -> tcpChannel.pipeline().fireUserEventTriggered(message));
    }

    /**
     * 关闭指定 InputChannel 对此客户端的使用。
     * 减少引用计数，当计数归零且不允许复用时关闭连接。
     */
    @Override
    public void close(RemoteInputChannel inputChannel) throws IOException {

        clientHandler.removeInputChannel(inputChannel);

        // 减少引用计数（确保不会小于 0），如果归零且不允许复用则关闭连接
        if (closeReferenceCounter.updateAndGet(count -> Math.max(count - 1, 0)) == 0
                && !canBeReused()) {
            closeConnection();
        } else {
            // 仍有其他 Channel 使用，只取消当前 Channel 的请求
            clientHandler.cancelRequestFor(inputChannel.getInputChannelId());
        }
    }

    /**
     * 关闭底层 TCP 连接。
     * 发送 CloseRequest 确保服务端正确处理未完成的事件，然后从工厂移除此客户端。
     */
    public void closeConnection() {
        Preconditions.checkState(
                canBeDisposed(), "The connection should not be closed before disposed.");
        if (closed.getAndSet(true)) {
            // 避免重复关闭
            // Do not close connection repeatedly
            return;
        }
        // 发送关闭请求，确保未完成的反向 Task 事件不被丢弃
        // Close the TCP connection. Send a close request msg to ensure
        // that outstanding backwards task events are not discarded.
        tcpChannel
                .writeAndFlush(new NettyMessage.CloseRequest())
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        // 从工厂移除此客户端
        // Make sure to remove the client from the factory
        clientFactory.destroyPartitionRequestClient(connectionId, this);
    }

    private boolean canBeReused() {
        return clientFactory.isConnectionReuseEnabled() && !clientHandler.hasChannelError();
    }

    private void checkNotClosed() throws IOException {
        if (closed.get()) {
            final SocketAddress localAddr = tcpChannel.localAddress();
            final SocketAddress remoteAddr = tcpChannel.remoteAddress();
            throw new LocalTransportException(
                    String.format(
                            "Channel to '%s [%s]' closed.",
                            remoteAddr, connectionId.getResourceID().getStringWithMetadata()),
                    localAddr);
        }
    }

    /**
     * AddCredit 消息包装类：封装 Credit 增量通知。
     * buildMessage() 时获取并重置 InputChannel 的未公告 Credit，
     * 如果 Credit 为 0 则不发送（返回 null）。
     */
    private static class AddCreditMessage extends ClientOutboundMessage {

        private AddCreditMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            // 获取并重置累积的 Credit，避免频繁发送小量 Credit
            int credits = inputChannel.getAndResetUnannouncedCredit();
            return credits > 0
                    ? new NettyMessage.AddCredit(credits, inputChannel.getInputChannelId())
                    : null;
        }
    }

    /**
     * NewBufferSize 消息包装类：通知服务端新的 Buffer 大小。
     * 用于动态调整网络传输的 Buffer 大小配置。
     */
    private static class NewBufferSizeMessage extends ClientOutboundMessage {
        private final int bufferSize;

        private NewBufferSizeMessage(RemoteInputChannel inputChannel, int bufferSize) {
            super(checkNotNull(inputChannel));
            this.bufferSize = bufferSize;
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.NewBufferSize(bufferSize, inputChannel.getInputChannelId());
        }
    }

    /**
     * ResumeConsumption 消息包装类：恢复数据消费。
     * 用于对齐 Checkpoint 完成后恢复被阻塞的 Channel。
     */
    private static class ResumeConsumptionMessage extends ClientOutboundMessage {

        private ResumeConsumptionMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.ResumeConsumption(inputChannel.getInputChannelId());
        }
    }

    /**
     * AckAllUserRecordsProcessed 消息包装类：确认所有用户记录已处理。
     * 用于 EndOfData 事件的同步确认。
     */
    private static class AcknowledgeAllRecordsProcessedMessage extends ClientOutboundMessage {

        private AcknowledgeAllRecordsProcessedMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.AckAllUserRecordsProcessed(inputChannel.getInputChannelId());
        }
    }

    /**
     * SegmentId 消息包装类：通知需要的 Segment ID。
     * 用于 Tiered Storage 场景，指定要读取的数据段。
     */
    private static class SegmentIdMessage extends ClientOutboundMessage {

        private final int segmentId;

        private final int subpartitionIndex;

        private SegmentIdMessage(
                RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {
            super(checkNotNull(inputChannel));
            this.subpartitionIndex = subpartitionIndex;
            this.segmentId = segmentId;
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.SegmentId(
                    subpartitionIndex, segmentId, inputChannel.getInputChannelId());
        }
    }
}
