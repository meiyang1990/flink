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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 【学习笔记】CreditBasedPartitionRequestClientHandler - 基于 Credit 的客户端消息处理器
 *
 * <p>核心职责：
 * 1. 处理来自上游 TaskManager 的数据响应（BufferResponse）和错误响应（ErrorResponse）
 * 2. 向上游发送 Credit 通告和消费恢复请求，实现 Credit-based 流量控制
 * 3. 管理本地 RemoteInputChannel 的注册和消息分发
 *
 * <p>Credit-based 流量控制机制：
 * - 下游通过 AddCredit 消息告知上游可用的缓冲区数量
 * - 上游只在有足够 Credit 时才发送数据，避免下游内存溢出
 * - 这种机制替代了传统的 TCP 背压，提供更细粒度的流量控制
 *
 * <p>消息处理：
 * - BufferResponse：接收数据缓冲区，分发到对应的 RemoteInputChannel
 * - ErrorResponse：处理错误，区分致命错误（关闭所有 Channel）和非致命错误（仅影响单个 Channel）
 * - BacklogAnnouncement：接收积压通告，触发缓冲区申请
 *
 * <p>线程安全：
 * - inputChannels 使用 ConcurrentHashMap 保证线程安全
 * - channelError 使用 AtomicReference 保证错误状态的原子性更新
 */
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter
        implements NetworkClientHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

    // 已注册的输入通道，key 为 InputChannelID
    private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels =
            new ConcurrentHashMap<>();

    // 待发送的出站消息队列（Credit 通告或恢复消费请求）
    private final ArrayDeque<ClientOutboundMessage> clientOutboundMessages = new ArrayDeque<>();

    // 通道级错误状态，一旦设置则拒绝所有后续操作
    private final AtomicReference<Throwable> channelError = new AtomicReference<>();

    // 写入完成后的回调监听器
    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    // Netty Channel 上下文，由 Netty 线程初始化
    private volatile ChannelHandlerContext ctx;

    // 连接标识
    private ConnectionID connectionID;

    // ------------------------------------------------------------------------
    // 输入通道注册管理
    // ------------------------------------------------------------------------

    // 注册输入通道，建立 ChannelID 到 RemoteInputChannel 的映射
    @Override
    public void addInputChannel(RemoteInputChannel listener) throws IOException {
        checkError();
        inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
    }

    @Override
    public void removeInputChannel(RemoteInputChannel listener) {
        inputChannels.remove(listener.getInputChannelId());
    }

    @Override
    public RemoteInputChannel getInputChannel(InputChannelID inputChannelId) {
        return inputChannels.get(inputChannelId);
    }

    // 向服务端发送取消分区请求
    @Override
    public void cancelRequestFor(InputChannelID inputChannelId) {
        if (inputChannelId == null || ctx == null) {
            return;
        }
        ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
    }

    // ------------------------------------------------------------------------
    // 网络事件处理
    // ------------------------------------------------------------------------

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }
        super.channelActive(ctx);
    }

    // 连接断开时通知所有通道并关闭
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final SocketAddress remoteAddr = ctx.channel().remoteAddress();

        notifyAllChannelsOfErrorAndClose(
                new RemoteTransportException(
                        "Connection unexpectedly closed by remote task manager '"
                                + remoteAddr
                                + " [ "
                                + connectionID.getResourceID().getStringWithMetadata()
                                + " ] "
                                + "'. "
                                + "This might indicate that the remote task manager was lost.",
                        remoteAddr));

        super.channelInactive(ctx);
    }

    // 异常处理：区分传输异常和连接重置
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof TransportException) {
            notifyAllChannelsOfErrorAndClose(cause);
        } else {
            final SocketAddress remoteAddr = ctx.channel().remoteAddress();

            final TransportException tex;

            // 优化 "Connection reset by peer" 的错误提示
            if (cause.getMessage() != null
                    && cause.getMessage().contains("Connection reset by peer")) {
                tex =
                        new RemoteTransportException(
                                "Lost connection to task manager '"
                                        + remoteAddr
                                        + " [ "
                                        + connectionID.getResourceID().getStringWithMetadata()
                                        + " ] "
                                        + "'. "
                                        + "This indicates that the remote task manager was lost.",
                                remoteAddr,
                                cause);
            } else {
                final SocketAddress localAddr = ctx.channel().localAddress();
                tex =
                        new LocalTransportException(
                                String.format(
                                        "%s (connection to '%s [%s]')",
                                        cause.getMessage(),
                                        remoteAddr,
                                        connectionID.getResourceID().getStringWithMetadata()),
                                localAddr,
                                cause);
            }

            notifyAllChannelsOfErrorAndClose(tex);
        }
    }

    // 读取消息入口
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            decodeMsg(msg);
        } catch (Throwable t) {
            notifyAllChannelsOfErrorAndClose(t);
        }
    }

    // 用户事件触发：处理 Credit 通告和连接错误
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ClientOutboundMessage) {
            boolean triggerWrite = clientOutboundMessages.isEmpty();

            clientOutboundMessages.add((ClientOutboundMessage) msg);

            // 队列为空时触发写入
            if (triggerWrite) {
                writeAndFlushNextMessageIfPossible(ctx.channel());
            }
        } else if (msg instanceof ConnectionErrorMessage) {
            notifyAllChannelsOfErrorAndClose(((ConnectionErrorMessage) msg).getCause());
        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    @Override
    public boolean hasChannelError() {
        return channelError.get() != null;
    }

    @Override
    public void setConnectionId(ConnectionID connectionId) {
        this.connectionID = checkNotNull(connectionId);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    @VisibleForTesting
    void notifyAllChannelsOfErrorAndClose(Throwable cause) {
        if (channelError.compareAndSet(null, cause)) {
            try {
                for (RemoteInputChannel inputChannel : inputChannels.values()) {
                    inputChannel.onError(cause);
                }
            } catch (Throwable t) {
                // We can only swallow the Exception at this point. :(
                LOG.warn(
                        "An Exception was thrown during error notification of a remote input channel.",
                        t);
            } finally {
                inputChannels.clear();
                clientOutboundMessages.clear();

                if (ctx != null) {
                    ctx.close();
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // 消息解码与分发
    // ------------------------------------------------------------------------

    // 解码并分发来自服务端的消息
    private void decodeMsg(Object msg) {
        final Class<?> msgClazz = msg.getClass();

        // ---- 数据缓冲区响应 --------------------------------------------------------
        if (msgClazz == NettyMessage.BufferResponse.class) {
            NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

            // 查找对应的输入通道
            RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
            if (inputChannel == null || inputChannel.isReleased()) {
                // 通道已释放，回收缓冲区并取消请求
                bufferOrEvent.releaseBuffer();
                cancelRequestFor(bufferOrEvent.receiverId);
                return;
            }

            try {
                decodeBufferOrEvent(inputChannel, bufferOrEvent);
            } catch (Throwable t) {
                inputChannel.onError(t);
            }

        } else if (msgClazz == NettyMessage.ErrorResponse.class) {
            // ---- 错误响应 ---------------------------------------------------------
            NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

            SocketAddress remoteAddr = ctx.channel().remoteAddress();

            if (error.isFatalError()) {
                // 致命错误：关闭所有通道
                notifyAllChannelsOfErrorAndClose(
                        new RemoteTransportException(
                                "Fatal error at remote task manager '"
                                        + remoteAddr
                                        + " [ "
                                        + connectionID.getResourceID().getStringWithMetadata()
                                        + " ] "
                                        + "'.",
                                remoteAddr,
                                error.cause));
            } else {
                // 非致命错误：仅影响单个通道
                RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

                if (inputChannel != null) {
                    if (error.cause.getClass() == PartitionNotFoundException.class) {
                        inputChannel.onFailedPartitionRequest();
                    } else {
                        inputChannel.onError(
                                new RemoteTransportException(
                                        "Error at remote task manager '"
                                                + remoteAddr
                                                + " [ "
                                                + connectionID
                                                        .getResourceID()
                                                        .getStringWithMetadata()
                                                + " ] "
                                                + "'.",
                                        remoteAddr,
                                        error.cause));
                    }
                }
            }
        } else if (msgClazz == NettyMessage.BacklogAnnouncement.class) {
            // ---- 积压通告：触发下游申请更多缓冲区 ------------------------------------
            NettyMessage.BacklogAnnouncement announcement = (NettyMessage.BacklogAnnouncement) msg;

            RemoteInputChannel inputChannel = inputChannels.get(announcement.receiverId);
            if (inputChannel == null || inputChannel.isReleased()) {
                cancelRequestFor(announcement.receiverId);
                return;
            }

            try {
                inputChannel.onSenderBacklog(announcement.backlog);
            } catch (Throwable throwable) {
                inputChannel.onError(throwable);
            }
        } else {
            throw new IllegalStateException(
                    "Received unknown message from producer: " + msg.getClass());
        }
    }

    private void decodeBufferOrEvent(
            RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent)
            throws Throwable {
        if (bufferOrEvent.isBuffer() && bufferOrEvent.bufferSize == 0) {
            inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
        } else if (bufferOrEvent.getBuffer() != null) {
            if (bufferOrEvent.numOfPartialBuffers > 0) {
                int offset = 0;

                int seq = bufferOrEvent.sequenceNumber;
                AtomicInteger waitToBeReleased =
                        new AtomicInteger(bufferOrEvent.numOfPartialBuffers);
                AtomicInteger processedPartialBuffers = new AtomicInteger(0);
                try {
                    for (int i = 0; i < bufferOrEvent.numOfPartialBuffers; i++) {
                        int size = bufferOrEvent.getPartialBufferSizes().get(i);

                        processedPartialBuffers.incrementAndGet();
                        inputChannel.onBuffer(
                                sliceBuffer(
                                        bufferOrEvent,
                                        memorySegment -> {
                                            if (waitToBeReleased.decrementAndGet() == 0) {
                                                bufferOrEvent.getBuffer().recycleBuffer();
                                            }
                                        },
                                        offset,
                                        size),
                                seq++,
                                i == bufferOrEvent.numOfPartialBuffers - 1
                                        ? bufferOrEvent.backlog
                                        : -1,
                                -1);
                        offset += size;
                    }
                } catch (Throwable throwable) {
                    LOG.error("Failed to process partial buffers.", throwable);
                    if (processedPartialBuffers.get() != bufferOrEvent.numOfPartialBuffers) {
                        bufferOrEvent.getBuffer().recycleBuffer();
                    }
                    throw throwable;
                }
            } else {
                inputChannel.onBuffer(
                        bufferOrEvent.getBuffer(),
                        bufferOrEvent.sequenceNumber,
                        bufferOrEvent.backlog,
                        bufferOrEvent.subpartitionId);
            }

        } else {
            throw new IllegalStateException(
                    "The read buffer is null in credit-based input channel.");
        }
    }

    /**
     * Creates a {@link NetworkBuffer} by wrapping the specified portion of a given buffer's
     * underlying memory segment rather than creating a slice of the buffer.
     *
     * <p>Currently, there is an assumption that each buffer received from a {@link
     * RemoteInputChannel} exclusively holds a single memory segment object.
     *
     * <p>If this assumption were violated and multiple buffers were allowed to share a single
     * segment, it could introduce instability and unpredictable behavior.
     *
     * <p>For instance, the BufferManager releases buffers by directly operating on their underlying
     * memory segments and adding them to a list designated for release. If buffers share the same
     * segment, the segment might be added to the buffer pool multiple times, and subsequent buffers
     * may inadvertently be allocated to the same segment for reading and writing.
     *
     * <p>Therefore, to avoid introducing potential risks, this method operates directly on the
     * segment instead of slicing the buffer.
     *
     * @param bufferOrEvent the buffer or event containing the data to be wrapped into a network
     *     buffer
     * @param recycler the buffer recycler used to manage the lifecycle of the network buffer
     * @param offset the offset within the buffer where the data begins
     * @param size the size of the data to be wrapped
     * @return a new {@link NetworkBuffer} wrapping the specified portion of the buffer's memory
     *     segment
     */
    private static NetworkBuffer sliceBuffer(
            NettyMessage.BufferResponse bufferOrEvent,
            BufferRecycler recycler,
            int offset,
            int size) {
        ByteBuffer nioBuffer = bufferOrEvent.getBuffer().getNioBuffer(offset, size);

        MemorySegment segment;
        if (nioBuffer.isDirect()) {
            segment = MemorySegmentFactory.wrapOffHeapMemory(nioBuffer);
        } else {
            byte[] bytes = nioBuffer.array();
            segment = MemorySegmentFactory.wrap(bytes);
        }

        return new NetworkBuffer(
                segment, recycler, bufferOrEvent.dataType, bufferOrEvent.isCompressed, size);
    }

    /**
     * Tries to write&flush unannounced credits for the next input channel in queue.
     *
     * <p>This method may be called by the first input channel enqueuing, or the complete future's
     * callback in previous input channel, or the channel writability changed event.
     */
    private void writeAndFlushNextMessageIfPossible(Channel channel) {
        if (channelError.get() != null || !channel.isWritable()) {
            return;
        }

        while (true) {
            ClientOutboundMessage outboundMessage = clientOutboundMessages.poll();

            // The input channel may be null because of the write callbacks
            // that are executed after each write.
            if (outboundMessage == null) {
                return;
            }

            // It is no need to notify credit or resume data consumption for the released channel.
            if (!outboundMessage.inputChannel.isReleased()) {
                Object msg = outboundMessage.buildMessage();
                if (msg == null) {
                    continue;
                }

                // Write and flush and wait until this is done before
                // trying to continue with the next input channel.
                channel.writeAndFlush(msg).addListener(writeListener);

                return;
            }
        }
    }

    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else if (future.cause() != null) {
                    notifyAllChannelsOfErrorAndClose(future.cause());
                } else {
                    notifyAllChannelsOfErrorAndClose(
                            new IllegalStateException("Sending cancelled by user."));
                }
            } catch (Throwable t) {
                notifyAllChannelsOfErrorAndClose(t);
            }
        }
    }
}
