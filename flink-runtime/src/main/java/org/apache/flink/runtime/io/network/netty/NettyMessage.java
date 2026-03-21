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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FileRegionBuffer;
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Netty 消息基类 - 定义 Flink 网络数据传输的所有消息类型和序列化协议。
 *
 * <p>消息帧结构：
 * <pre>
 *   +------------------+------------------+--------++------------------+
 *   | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || MESSAGE CONTENT  |
 *   +------------------+------------------+--------++------------------+
 *   |<----- FRAME_HEADER_LENGTH (9) ----->|        |<- 消息体长度可变 ->|
 * </pre>
 *
 * <p>消息类型（按 ID 分类）：
 *
 * <p><b>服务端响应消息：</b>
 * <ul>
 *   <li>BufferResponse (ID=0)：数据缓冲响应，携带实际的 Shuffle 数据</li>
 *   <li>ErrorResponse (ID=1)：错误响应，包含异常信息</li>
 *   <li>BacklogAnnouncement (ID=9)：积压通知，告知下游有多少待发数据</li>
 * </ul>
 *
 * <p><b>客户端请求消息：</b>
 * <ul>
 *   <li>PartitionRequest (ID=2)：分区请求，建立数据通道</li>
 *   <li>TaskEventRequest (ID=3)：Task 事件请求，反向发送事件到上游</li>
 *   <li>CancelPartitionRequest (ID=4)：取消分区请求</li>
 *   <li>CloseRequest (ID=5)：关闭连接请求</li>
 *   <li>AddCredit (ID=6)：增量 Credit 通知（流量控制核心）</li>
 *   <li>ResumeConsumption (ID=7)：恢复消费（Checkpoint 后）</li>
 *   <li>AckAllUserRecordsProcessed (ID=8)：确认所有用户记录已处理</li>
 *   <li>NewBufferSize (ID=10)：新 Buffer 大小通知</li>
 *   <li>SegmentId (ID=11)：Segment ID 请求（Tiered Storage）</li>
 * </ul>
 *
 * <p>协议要点：
 * <ul>
 *   <li>MAGIC_NUMBER (0xBADC0FFE)：用于校验帧完整性，检测流损坏</li>
 *   <li>每种消息子类型需要有 public 无参构造函数（用于反序列化）</li>
 *   <li>消息编码时先写帧头（长度+魔数+ID），再写消息体</li>
 *   <li>BufferResponse 特殊处理：支持零拷贝发送（FileRegionBuffer）</li>
 * </ul>
 *
 * A simple and generic interface to serialize messages to Netty's buffer space.
 *
 * <p>This class must be public as long as we are using a Netty version prior to 4.0.45. Please
 * check FLINK-7845 for more information.
 */
public abstract class NettyMessage {

    // ------------------------------------------------------------------------
    // 注意：每个 NettyMessage 子类都需要有 public 无参构造函数，用于通用反序列化
    // Note: Every NettyMessage subtype needs to have a public 0-argument
    // constructor in order to work with the generic deserializer.
    // ------------------------------------------------------------------------

    /**
     * 帧头长度：帧长度(4) + 魔数(4) + 消息ID(1) = 9 字节。
     * frame length (4), magic number (4), msg ID (1)
     */
    static final int FRAME_HEADER_LENGTH =
            4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

    /** 魔数：用于校验帧完整性，检测网络流损坏 */
    static final int MAGIC_NUMBER = 0xBADC0FFE;

    /**
     * 将消息写入 Netty Channel。
     * 每个子类实现自己的序列化逻辑。
     */
    abstract void write(
            ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
            throws IOException;

    // ------------------------------------------------------------------------

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>Before sending the buffer, you must write the actual length after adding the contents as
     * an integer to position <tt>0</tt>!
     *
     * @param allocator byte buffer allocator to use
     * @param id {@link NettyMessage} subclass ID
     * @return a newly allocated direct buffer with header data written for {@link
     *     NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
        return allocateBuffer(allocator, id, -1);
    }

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator byte buffer allocator to use
     * @param id {@link NettyMessage} subclass ID
     * @param contentLength content length (or <tt>-1</tt> if unknown)
     * @return a newly allocated direct buffer with header data written for {@link
     *     NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
        return allocateBuffer(allocator, id, 0, contentLength, true);
    }

    /**
     * Allocates a new buffer and adds some header information for the frame decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator byte buffer allocator to use
     * @param id {@link NettyMessage} subclass ID
     * @param messageHeaderLength additional header length that should be part of the allocated
     *     buffer and is written outside of this method
     * @param contentLength content length (or <tt>-1</tt> if unknown)
     * @param allocateForContent whether to make room for the actual content in the buffer
     *     (<tt>true</tt>) or whether to only return a buffer with the header information
     *     (<tt>false</tt>)
     * @return a newly allocated direct buffer with header data written for {@link
     *     NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(
            ByteBufAllocator allocator,
            byte id,
            int messageHeaderLength,
            int contentLength,
            boolean allocateForContent) {
        checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

        final ByteBuf buffer;
        if (!allocateForContent) {
            buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength);
        } else if (contentLength != -1) {
            buffer =
                    allocator.directBuffer(
                            FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
        } else {
            // content length unknown -> start with the default initial size (rather than
            // FRAME_HEADER_LENGTH only):
            buffer = allocator.directBuffer();
        }
        buffer.writeInt(
                FRAME_HEADER_LENGTH
                        + messageHeaderLength
                        + contentLength); // may be updated later, e.g. if contentLength == -1
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeByte(id);

        return buffer;
    }

    // ------------------------------------------------------------------------
    // 通用 NettyMessage 编码器和解码器
    // Generic NettyMessage encoder and decoder
    // ------------------------------------------------------------------------

    /**
     * 消息编码器：将 NettyMessage 序列化到 Netty ByteBuf。
     * 可共享（@Sharable），因为编码逻辑是无状态的。
     */
    @ChannelHandler.Sharable
    static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws IOException {
            if (msg instanceof NettyMessage) {
                // 调用消息的 write 方法进行序列化
                ((NettyMessage) msg).write(ctx, promise, ctx.alloc());
            } else {
                // 非 NettyMessage 直接传递
                ctx.write(msg, promise);
            }
        }
    }

    /**
     * 消息解码器：基于帧长度的解码器，将 ByteBuf 解析为 NettyMessage。
     *
     * <p>帧结构（由 allocateBuffer 方法创建）：
     * <pre>
     * +------------------+------------------+--------++----------------+
     * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
     * +------------------+------------------+--------++----------------+
     * </pre>
     *
     * <p>解码流程：
     * <ol>
     *   <li>父类 LengthFieldBasedFrameDecoder 根据帧长度字段切分完整帧</li>
     *   <li>校验 MAGIC_NUMBER 确保帧完整性</li>
     *   <li>根据消息 ID 调用对应子类的 readFrom 方法反序列化</li>
     * </ol>
     *
     * Message decoder based on netty's {@link LengthFieldBasedFrameDecoder} but avoiding the
     * additional memory copy inside {@link #extractFrame(ChannelHandlerContext, ByteBuf, int, int)}
     * since we completely decode the {@link ByteBuf} inside {@link #decode(ChannelHandlerContext,
     * ByteBuf)} and will not re-use it afterwards.
     *
     * <p>The frame-length encoder will be based on this transmission scheme created by {@link
     * NettyMessage#allocateBuffer(ByteBufAllocator, byte, int)}:
     *
     * <pre>
     * +------------------+------------------+--------++----------------+
     * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
     * +------------------+------------------+--------++----------------+
     * </pre>
     */
    static class NettyMessageDecoder extends LengthFieldBasedFrameDecoder {
        /**
         * 创建消息解码器，配置帧属性。
         * 参数含义：maxFrameLength=MAX_INT, lengthFieldOffset=0, lengthFieldLength=4,
         *         lengthAdjustment=-4（帧长度包含自身4字节）, initialBytesToStrip=4（跳过长度字段）
         * Creates a new message decoded with the required frame properties.
         */
        NettyMessageDecoder() {
            super(Integer.MAX_VALUE, 0, 4, -4, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            // 先由父类切分出完整的帧
            ByteBuf msg = (ByteBuf) super.decode(ctx, in);
            if (msg == null) {
                return null;
            }

            try {
                // 校验魔数，确保帧未被损坏
                int magicNumber = msg.readInt();

                if (magicNumber != MAGIC_NUMBER) {
                    throw new IllegalStateException(
                            "Network stream corrupted: received incorrect magic number.");
                }

                // 读取消息类型 ID
                byte msgId = msg.readByte();

                // 根据 ID 分发到对应的反序列化方法
                final NettyMessage decodedMsg;
                switch (msgId) {
                    case PartitionRequest.ID:
                        decodedMsg = PartitionRequest.readFrom(msg);
                        break;
                    case TaskEventRequest.ID:
                        decodedMsg = TaskEventRequest.readFrom(msg, getClass().getClassLoader());
                        break;
                    case CancelPartitionRequest.ID:
                        decodedMsg = CancelPartitionRequest.readFrom(msg);
                        break;
                    case CloseRequest.ID:
                        decodedMsg = CloseRequest.readFrom(msg);
                        break;
                    case AddCredit.ID:
                        decodedMsg = AddCredit.readFrom(msg);
                        break;
                    case ResumeConsumption.ID:
                        decodedMsg = ResumeConsumption.readFrom(msg);
                        break;
                    case AckAllUserRecordsProcessed.ID:
                        decodedMsg = AckAllUserRecordsProcessed.readFrom(msg);
                        break;
                    case NewBufferSize.ID:
                        decodedMsg = NewBufferSize.readFrom(msg);
                        break;
                    case SegmentId.ID:
                        decodedMsg = SegmentId.readFrom(msg);
                        break;
                    default:
                        throw new ProtocolException(
                                "Received unknown message from producer: " + msg);
                }

                return decodedMsg;
            } finally {
                // 释放 ByteBuf（BufferResponse 已经 retain 过，这里释放是安全的）
                // ByteToMessageDecoder cleanup (only the BufferResponse holds on to the decoded
                // msg but already retain()s the buffer once)
                msg.release();
            }
        }
    }

    // ------------------------------------------------------------------------
    // 服务端响应消息
    // Server responses
    // ------------------------------------------------------------------------

    /**
     * 数据缓冲响应消息 - 携带实际的 Shuffle 数据。
     *
     * <p>这是 Flink 网络传输中最重要的消息类型，承载算子间交换的数据。
     *
     * <p>消息头结构（MESSAGE_HEADER_LENGTH = 38 字节）：
     * <pre>
     *   receiverId (16)        - 接收者 InputChannel ID
     *   subpartitionId (4)     - 子分区索引
     *   numOfPartialBuffers (4)- 部分缓冲数量（用于 FullyFilledBuffer）
     *   sequenceNumber (4)     - 序列号，用于检测丢包和乱序
     *   backlog (4)            - 积压数量，告知下游还有多少待发数据
     *   dataType (1)           - 数据类型（Buffer、Event 等）
     *   isCompressed (1)       - 是否压缩
     *   bufferSize (4)         - 缓冲大小
     *   [partialBufferSizes]   - 可选：部分缓冲的各自大小
     * </pre>
     *
     * <p>数据类型说明：
     * <ul>
     *   <li>DATA_BUFFER：普通数据</li>
     *   <li>EVENT_BUFFER：事件（如 Checkpoint Barrier、EndOfPartition）</li>
     *   <li>TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER：可超时的对齐 Barrier</li>
     * </ul>
     *
     * <p>序列化特点：
     * <ul>
     *   <li>消息头和数据体分开写入，支持零拷贝发送</li>
     *   <li>FileRegionBuffer 可直接发送文件内容，避免用户态拷贝</li>
     * </ul>
     */
    static class BufferResponse extends NettyMessage {

        static final byte ID = 0;

        /**
         * 消息头长度：receiverId(16) + subpartitionId(4) + numOfPartialBuffers(4)
         *          + sequenceNumber(4) + backlog(4) + dataType(1) + isCompressed(1) + bufferSize(4) = 38 字节
         * receiver ID (16), sequence number (4), backlog (4), subpartition id (4), partial buffers
         * number (4), dataType (1), isCompressed (1), buffer size (4)
         */
        static final int MESSAGE_HEADER_LENGTH =
                InputChannelID.getByteBufLength()
                        + Integer.BYTES
                        + Integer.BYTES
                        + Integer.BYTES
                        + Integer.BYTES
                        + Byte.BYTES
                        + Byte.BYTES
                        + Integer.BYTES;

        /** 数据缓冲，包含实际的 Shuffle 数据 */
        final Buffer buffer;

        /** 接收者 InputChannel ID，用于路由到正确的 Channel */
        final InputChannelID receiverId;

        /** 子分区索引，标识数据来自哪个子分区 */
        final int subpartitionId;

        /** 序列号，用于检测丢包和乱序 */
        final int sequenceNumber;

        /** 积压数量，告知下游还有多少待发数据，用于流量控制 */
        final int backlog;

        /** 数据类型（Buffer、Event 等） */
        final Buffer.DataType dataType;

        /** 是否压缩 */
        final boolean isCompressed;

        /** 缓冲大小 */
        final int bufferSize;

        /** 部分缓冲数量（用于 FullyFilledBuffer 场景） */
        final int numOfPartialBuffers;

        /** 部分缓冲的各自大小列表 */
        private List<Integer> partialBufferSizes = new ArrayList<>();

        private BufferResponse(
                @Nullable Buffer buffer,
                Buffer.DataType dataType,
                boolean isCompressed,
                int sequenceNumber,
                InputChannelID receiverId,
                int subpartitionId,
                int numOfPartialBuffers,
                int backlog,
                int bufferSize) {
            this.buffer = buffer;
            this.dataType = dataType;
            this.isCompressed = isCompressed;
            this.sequenceNumber = sequenceNumber;
            this.receiverId = checkNotNull(receiverId);
            this.subpartitionId = subpartitionId;
            this.backlog = backlog;
            this.bufferSize = bufferSize;
            this.numOfPartialBuffers = numOfPartialBuffers;
        }

        BufferResponse(
                Buffer buffer,
                int sequenceNumber,
                InputChannelID receiverId,
                int subpartitionId,
                int numOfPartialBuffers,
                int backlog) {
            this.buffer = checkNotNull(buffer);
            checkArgument(
                    buffer.getDataType().ordinal() <= Byte.MAX_VALUE,
                    "Too many data types defined!");
            checkArgument(backlog >= 0, "Must be non-negative.");
            this.dataType = buffer.getDataType();
            this.isCompressed = buffer.isCompressed();
            this.sequenceNumber = sequenceNumber;
            this.receiverId = checkNotNull(receiverId);
            this.subpartitionId = subpartitionId;
            this.backlog = backlog;
            this.bufferSize = buffer.getSize();
            this.numOfPartialBuffers = numOfPartialBuffers;
        }

        boolean isBuffer() {
            return dataType.isBuffer();
        }

        @Nullable
        public Buffer getBuffer() {
            return buffer;
        }

        void releaseBuffer() {
            if (buffer != null) {
                buffer.recycleBuffer();
            }
        }

        public List<Integer> getPartialBufferSizes() {
            return partialBufferSizes;
        }

        // --------------------------------------------------------------------
        // Serialization
        // --------------------------------------------------------------------

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf headerBuf = null;
            try {
                // in order to forward the buffer to netty, it needs an allocator set
                buffer.setAllocator(allocator);

                headerBuf = fillHeader(allocator);
                out.write(headerBuf);
                if (buffer instanceof FileRegionBuffer) {
                    out.write(buffer, promise);
                } else {
                    out.write(buffer.asByteBuf(), promise);
                }
            } catch (Throwable t) {
                handleException(headerBuf, buffer, t);
            }
        }

        @VisibleForTesting
        ByteBuf write(ByteBufAllocator allocator) throws IOException {
            ByteBuf headerBuf = null;
            try {
                // in order to forward the buffer to netty, it needs an allocator set
                buffer.setAllocator(allocator);

                headerBuf = fillHeader(allocator);

                CompositeByteBuf composityBuf = allocator.compositeDirectBuffer();
                composityBuf.addComponent(headerBuf);
                composityBuf.addComponent(buffer.asByteBuf());
                // update writer index since we have data written to the components:
                composityBuf.writerIndex(
                        headerBuf.writerIndex() + buffer.asByteBuf().writerIndex());
                return composityBuf;
            } catch (Throwable t) {
                handleException(headerBuf, buffer, t);
                return null; // silence the compiler
            }
        }

        private ByteBuf fillHeader(ByteBufAllocator allocator) {
            // only allocate header buffer - we will combine it with the data buffer below
            ByteBuf headerBuf =
                    allocateBuffer(
                            allocator,
                            ID,
                            MESSAGE_HEADER_LENGTH + Integer.BYTES * numOfPartialBuffers,
                            bufferSize,
                            false);

            receiverId.writeTo(headerBuf);
            headerBuf.writeInt(subpartitionId);
            headerBuf.writeInt(numOfPartialBuffers);
            headerBuf.writeInt(sequenceNumber);
            headerBuf.writeInt(backlog);
            headerBuf.writeByte(dataType.ordinal());
            headerBuf.writeBoolean(isCompressed);
            headerBuf.writeInt(buffer.readableBytes());

            if (numOfPartialBuffers > 0) {
                checkArgument(
                        buffer instanceof FullyFilledBuffer,
                        "Partial buffers are only supported for fully filled buffers.");
                List<Buffer> partialBuffers = ((FullyFilledBuffer) buffer).getPartialBuffers();
                checkArgument(
                        partialBuffers.size() == numOfPartialBuffers,
                        "Mismatched number of partial buffers");
                for (int i = 0; i < numOfPartialBuffers; i++) {
                    int bytes = partialBuffers.get(i).readableBytes();
                    headerBuf.writeInt(bytes);
                }
            }

            return headerBuf;
        }

        /**
         * Parses the message header part and composes a new BufferResponse with an empty data
         * buffer. The data buffer will be filled in later.
         *
         * @param messageHeader the serialized message header.
         * @param bufferAllocator the allocator for network buffer.
         * @return a BufferResponse object with the header parsed and the data buffer to fill in
         *     later. The data buffer will be null if the target channel has been released or the
         *     buffer size is 0.
         */
        static BufferResponse readFrom(
                ByteBuf messageHeader, NetworkBufferAllocator bufferAllocator) {
            InputChannelID receiverId = InputChannelID.fromByteBuf(messageHeader);
            int subpartitionId = messageHeader.readInt();
            int numOfPartialBuffers = messageHeader.readInt();
            int sequenceNumber = messageHeader.readInt();
            int backlog = messageHeader.readInt();
            Buffer.DataType dataType = Buffer.DataType.values()[messageHeader.readByte()];
            boolean isCompressed = messageHeader.readBoolean();
            int size = messageHeader.readInt();

            Buffer dataBuffer;
            if (dataType.isBuffer()) {
                dataBuffer = bufferAllocator.allocatePooledNetworkBuffer(receiverId);
                if (dataBuffer != null) {
                    dataBuffer.setDataType(dataType);
                }
            } else {
                dataBuffer = bufferAllocator.allocateUnPooledNetworkBuffer(size, dataType);
            }

            if (size == 0 && dataBuffer != null) {
                // recycle the empty buffer directly, we must allocate a buffer for
                // the empty data to release the credit already allocated for it
                dataBuffer.recycleBuffer();
                dataBuffer = null;
            }

            if (dataBuffer != null) {
                dataBuffer.setCompressed(isCompressed);
            }

            return new BufferResponse(
                    dataBuffer,
                    dataType,
                    isCompressed,
                    sequenceNumber,
                    receiverId,
                    subpartitionId,
                    numOfPartialBuffers,
                    backlog,
                    size);
        }
    }

    /**
     * 错误响应消息 - 服务端向客户端返回的错误信息。
     *
     * <p>错误类型：
     * <ul>
     *   <li>Fatal Error（receiverId=null）：致命错误，影响整个连接</li>
     *   <li>Channel Error（receiverId!=null）：通道错误，只影响指定的 InputChannel</li>
     * </ul>
     *
     * <p>常见错误场景：
     * <ul>
     *   <li>请求的分区不存在</li>
     *   <li>分区已被释放</li>
     *   <li>子分区索引越界</li>
     *   <li>内部服务端异常</li>
     * </ul>
     */
    static class ErrorResponse extends NettyMessage {

        static final byte ID = 1;

        /** 错误原因，会被序列化传输 */
        final Throwable cause;

        /** 可选的接收者 ID，null 表示致命错误 */
        @Nullable final InputChannelID receiverId;

        /** 构造致命错误（影响整个连接） */
        ErrorResponse(Throwable cause) {
            this.cause = checkNotNull(cause);
            this.receiverId = null;
        }

        /** 构造通道错误（只影响指定 Channel） */
        ErrorResponse(Throwable cause, InputChannelID receiverId) {
            this.cause = checkNotNull(cause);
            this.receiverId = receiverId;
        }

        /** 判断是否为致命错误 */
        boolean isFatalError() {
            return receiverId == null;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID);

            try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
                oos.writeObject(cause);

                if (receiverId != null) {
                    result.writeBoolean(true);
                    receiverId.writeTo(result);
                } else {
                    result.writeBoolean(false);
                }

                // Update frame length...
                result.setInt(0, result.readableBytes());
                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
                Object obj = ois.readObject();

                if (!(obj instanceof Throwable)) {
                    throw new ClassCastException(
                            "Read object expected to be of type Throwable, "
                                    + "actual type is "
                                    + obj.getClass()
                                    + ".");
                } else {
                    if (buffer.readBoolean()) {
                        InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
                        return new ErrorResponse((Throwable) obj, receiverId);
                    } else {
                        return new ErrorResponse((Throwable) obj);
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // 客户端请求消息
    // Client requests
    // ------------------------------------------------------------------------

    /**
     * 分区请求消息 - 客户端向服务端请求建立数据通道。
     *
     * <p>这是建立 Shuffle 数据通道的第一个消息，RemoteInputChannel 发送此请求后，
     * 服务端会创建 ResultSubpartitionView 并开始发送数据。
     *
     * <p>消息内容：
     * <ul>
     *   <li>partitionId：目标分区标识（包含 IntermediateResultPartitionID 和 producerId）</li>
     *   <li>queueIndexSet：请求的子分区索引集合（支持多子分区合并读取）</li>
     *   <li>receiverId：接收者 InputChannel ID，服务端用于标识响应目标</li>
     *   <li>credit：初始 Credit，决定服务端可以立即发送多少 Buffer</li>
     * </ul>
     *
     * <p>处理流程：
     * <ol>
     *   <li>客户端 RemoteInputChannel 创建请求，通过 NettyPartitionRequestClient 发送</li>
     *   <li>服务端 PartitionRequestServerHandler 接收，通过 partitionProvider 获取 view</li>
     *   <li>创建 NetworkSequenceViewReader 并加入 PartitionRequestQueue</li>
     *   <li>PartitionRequestQueue 开始发送 BufferResponse</li>
     * </ol>
     */
    static class PartitionRequest extends NettyMessage {

        private static final byte ID = 2;

        /** 目标分区 ID（包含 partitionId 和 producerId） */
        final ResultPartitionID partitionId;

        /** 请求的子分区索引集合 */
        final ResultSubpartitionIndexSet queueIndexSet;

        /** 接收者 InputChannel ID */
        final InputChannelID receiverId;

        /** 初始 Credit，表示客户端可以接收多少 Buffer */
        final int credit;

        PartitionRequest(
                ResultPartitionID partitionId,
                ResultSubpartitionIndexSet queueIndexSet,
                InputChannelID receiverId,
                int credit) {
            this.partitionId = checkNotNull(partitionId);
            this.queueIndexSet = queueIndexSet;
            this.receiverId = checkNotNull(receiverId);
            this.credit = credit;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            Consumer<ByteBuf> consumer =
                    (bb) -> {
                        partitionId.getPartitionId().writeTo(bb);
                        partitionId.getProducerId().writeTo(bb);
                        queueIndexSet.writeTo(bb);
                        receiverId.writeTo(bb);
                        bb.writeInt(credit);
                    };

            writeToChannel(
                    out,
                    promise,
                    allocator,
                    consumer,
                    ID,
                    IntermediateResultPartitionID.getByteBufLength()
                            + ExecutionAttemptID.getByteBufLength()
                            + ResultSubpartitionIndexSet.getByteBufLength(queueIndexSet)
                            + InputChannelID.getByteBufLength()
                            + Integer.BYTES);
        }

        static PartitionRequest readFrom(ByteBuf buffer) {
            ResultPartitionID partitionId =
                    new ResultPartitionID(
                            IntermediateResultPartitionID.fromByteBuf(buffer),
                            ExecutionAttemptID.fromByteBuf(buffer));
            ResultSubpartitionIndexSet queueIndexSet =
                    ResultSubpartitionIndexSet.fromByteBuf(buffer);
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
            int credit = buffer.readInt();

            return new PartitionRequest(partitionId, queueIndexSet, receiverId, credit);
        }

        @Override
        public String toString() {
            return String.format("PartitionRequest(%s:%s:%d)", partitionId, queueIndexSet, credit);
        }
    }

    /**
     * Task 事件请求消息 - 反向发送事件到上游生产者。
     *
     * <p>在流水线执行中，下游 Task 可以通过此消息向上游发送事件，
     * 典型场景是迭代算法中的反馈通道。
     *
     * <p>注意：此消息只在生产者和消费者都运行时才有效（流水线执行模式）。
     */
    static class TaskEventRequest extends NettyMessage {

        private static final byte ID = 3;

        /** Task 事件内容 */
        final TaskEvent event;

        /** 接收者 InputChannel ID */
        final InputChannelID receiverId;

        /** 目标分区 ID */
        final ResultPartitionID partitionId;

        TaskEventRequest(
                TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) {
            this.event = checkNotNull(event);
            this.receiverId = checkNotNull(receiverId);
            this.partitionId = checkNotNull(partitionId);
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            // TODO Directly serialize to Netty's buffer
            ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

            Consumer<ByteBuf> consumer =
                    (bb) -> {
                        bb.writeInt(serializedEvent.remaining());
                        bb.writeBytes(serializedEvent);

                        partitionId.getPartitionId().writeTo(bb);
                        partitionId.getProducerId().writeTo(bb);
                        receiverId.writeTo(bb);
                    };

            writeToChannel(
                    out,
                    promise,
                    allocator,
                    consumer,
                    ID,
                    Integer.BYTES
                            + serializedEvent.remaining()
                            + IntermediateResultPartitionID.getByteBufLength()
                            + ExecutionAttemptID.getByteBufLength()
                            + InputChannelID.getByteBufLength());
        }

        static TaskEventRequest readFrom(ByteBuf buffer, ClassLoader classLoader)
                throws IOException {
            // directly deserialize fromNetty's buffer
            int length = buffer.readInt();
            ByteBuffer serializedEvent = buffer.nioBuffer(buffer.readerIndex(), length);
            // assume this event's content is read from the ByteBuf (positions are not shared!)
            buffer.readerIndex(buffer.readerIndex() + length);

            TaskEvent event =
                    (TaskEvent) EventSerializer.fromSerializedEvent(serializedEvent, classLoader);

            ResultPartitionID partitionId =
                    new ResultPartitionID(
                            IntermediateResultPartitionID.fromByteBuf(buffer),
                            ExecutionAttemptID.fromByteBuf(buffer));

            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new TaskEventRequest(event, partitionId, receiverId);
        }
    }

    /**
     * 取消分区请求消息 - 取消指定 InputChannel 的分区请求。
     *
     * <p>使用场景：
     * <ul>
     *   <li>Task 被取消时，取消正在进行的分区请求</li>
     *   <li>InputChannel 关闭时清理服务端资源</li>
     * </ul>
     *
     * <p>由于 InputChannel 和分区请求是 1:1 映射，InputChannelID 足以标识要取消的请求。
     *
     * Cancels the partition request of the {@link InputChannel} identified by {@link
     * InputChannelID}.
     *
     * <p>There is a 1:1 mapping between the input channel and partition per physical channel.
     * Therefore, the {@link InputChannelID} instance is enough to identify which request to cancel.
     */
    static class CancelPartitionRequest extends NettyMessage {

        private static final byte ID = 4;

        /** 要取消的 InputChannel ID */
        final InputChannelID receiverId;

        CancelPartitionRequest(InputChannelID receiverId) {
            this.receiverId = checkNotNull(receiverId);
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    receiverId::writeTo,
                    ID,
                    InputChannelID.getByteBufLength());
        }

        static CancelPartitionRequest readFrom(ByteBuf buffer) throws Exception {
            return new CancelPartitionRequest(InputChannelID.fromByteBuf(buffer));
        }
    }

    /**
     * 关闭请求消息 - 请求关闭网络连接。
     *
     * <p>使用场景：
     * <ul>
     *   <li>客户端主动关闭连接前发送，确保服务端正确处理未完成的事件</li>
     *   <li>主要目的是防止正在传输的反向 Task 事件被丢弃</li>
     * </ul>
     */
    static class CloseRequest extends NettyMessage {

        private static final byte ID = 5;

        CloseRequest() {}

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(out, promise, allocator, ignored -> {}, ID, 0);
        }

        static CloseRequest readFrom(@SuppressWarnings("unused") ByteBuf buffer) throws Exception {
            return new CloseRequest();
        }
    }

    /**
     * 增量 Credit 通知消息 - Credit-based 流量控制的核心机制。
     *
     * <p>工作原理：
     * <ol>
     *   <li>下游 InputChannel 获得新的可用缓冲区时，累积 Credit</li>
     *   <li>达到阈值或需要时，通过此消息通知上游</li>
     *   <li>上游收到后更新 Credit 计数，只有 Credit > 0 时才发送数据</li>
     * </ol>
     *
     * <p>背压机制：
     * <ul>
     *   <li>当下游处理慢时，缓冲区不够，无法提供新 Credit</li>
     *   <li>上游 Credit 耗尽后停止发送，实现背压</li>
     *   <li>下游处理完成释放缓冲区后，发送新 Credit 恢复传输</li>
     * </ul>
     *
     * Incremental credit announcement from the client to the server.
     */
    static class AddCredit extends NettyMessage {

        private static final byte ID = 6;

        /** Credit 增量值，必须 > 0 */
        final int credit;

        /** 接收者 InputChannel ID */
        final InputChannelID receiverId;

        AddCredit(int credit, InputChannelID receiverId) {
            checkArgument(credit > 0, "The announced credit should be greater than 0");
            this.credit = credit;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator, ID, Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(credit);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static AddCredit readFrom(ByteBuf buffer) {
            int credit = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new AddCredit(credit, receiverId);
        }

        @Override
        public String toString() {
            return String.format("AddCredit(%s : %d)", receiverId, credit);
        }
    }

    /**
     * 恢复消费消息 - 通知生产者从 Checkpoint 阻塞中恢复。
     *
     * <p>使用场景（对齐 Checkpoint）：
     * <ol>
     *   <li>下游收到某个 Channel 的 Barrier 后，暂停该 Channel 的消费</li>
     *   <li>等待所有 Channel 的 Barrier 对齐</li>
     *   <li>对齐完成后，发送此消息通知上游恢复发送</li>
     * </ol>
     *
     * <p>与非对齐 Checkpoint 的区别：
     * 非对齐 Checkpoint 不会暂停消费，因此不需要此消息。
     *
     * Message to notify the producer to unblock from checkpoint.
     */
    static class ResumeConsumption extends NettyMessage {

        private static final byte ID = 7;

        /** 要恢复消费的 InputChannel ID */
        final InputChannelID receiverId;

        ResumeConsumption(InputChannelID receiverId) {
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    receiverId::writeTo,
                    ID,
                    InputChannelID.getByteBufLength());
        }

        static ResumeConsumption readFrom(ByteBuf buffer) {
            return new ResumeConsumption(InputChannelID.fromByteBuf(buffer));
        }

        @Override
        public String toString() {
            return String.format("ResumeConsumption(%s)", receiverId);
        }
    }

    /**
     * 确认所有用户记录已处理消息 - 用于 EndOfData 事件同步。
     *
     * <p>使用场景：
     * <ul>
     *   <li>下游收到 EndOfData 事件后，确认所有之前的记录都已处理</li>
     *   <li>用于支持批流统一的 exactly-once 语义</li>
     * </ul>
     */
    static class AckAllUserRecordsProcessed extends NettyMessage {

        private static final byte ID = 8;

        /** 确认的 InputChannel ID */
        final InputChannelID receiverId;

        AckAllUserRecordsProcessed(InputChannelID receiverId) {
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    receiverId::writeTo,
                    ID,
                    InputChannelID.getByteBufLength());
        }

        static AckAllUserRecordsProcessed readFrom(ByteBuf buffer) {
            return new AckAllUserRecordsProcessed(InputChannelID.fromByteBuf(buffer));
        }

        @Override
        public String toString() {
            return String.format("AckAllUserRecordsProcessed(%s)", receiverId);
        }
    }

    /**
     * 积压通知消息 - 生产者向消费者通知待发送数据量（用于 Credit 分配）。
     *
     * <p>工作原理：
     * <ol>
     *   <li>生产者在没有 Credit 时，定期发送积压数量</li>
     *   <li>消费者收到后，可以决定是否/如何分配更多 Credit</li>
     *   <li>帮助消费者了解上游数据量，优化 Credit 分配策略</li>
     * </ol>
     *
     * <p>与 BufferResponse.backlog 的区别：
     * BufferResponse 中的 backlog 是发送数据时捎带的，而此消息是独立发送的。
     *
     * Backlog announcement from the producer to the consumer for credit allocation.
     */
    static class BacklogAnnouncement extends NettyMessage {

        static final byte ID = 9;

        /** 积压的 Buffer 数量，必须 > 0 */
        final int backlog;

        /** 接收者 InputChannel ID */
        final InputChannelID receiverId;

        BacklogAnnouncement(int backlog, InputChannelID receiverId) {
            checkArgument(backlog > 0, "Must be positive.");
            checkArgument(receiverId != null, "Must be not null.");

            this.backlog = backlog;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator, ID, Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(backlog);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static BacklogAnnouncement readFrom(ByteBuf buffer) {
            int backlog = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new BacklogAnnouncement(backlog, receiverId);
        }

        @Override
        public String toString() {
            return String.format("BacklogAnnouncement(%d : %s)", backlog, receiverId);
        }
    }

    /**
     * 新 Buffer 大小通知消息 - 动态调整网络传输的 Buffer 大小。
     *
     * <p>使用场景：
     * <ul>
     *   <li>运行时根据负载动态调整 Buffer 大小</li>
     *   <li>优化内存使用和传输效率</li>
     * </ul>
     *
     * Message to notify producer about new buffer size.
     */
    static class NewBufferSize extends NettyMessage {

        private static final byte ID = 10;

        /** 新的 Buffer 大小，必须 > 0 */
        final int bufferSize;

        /** 接收者 InputChannel ID */
        final InputChannelID receiverId;

        NewBufferSize(int bufferSize, InputChannelID receiverId) {
            checkArgument(bufferSize > 0, "The new buffer size should be greater than 0");
            this.bufferSize = bufferSize;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator, ID, Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(bufferSize);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static NewBufferSize readFrom(ByteBuf buffer) {
            int bufferSize = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new NewBufferSize(bufferSize, receiverId);
        }

        @Override
        public String toString() {
            return String.format("NewBufferSize(%s : %d)", receiverId, bufferSize);
        }
    }

    /**
     * Segment ID 通知消息 - 用于 Tiered Storage 场景。
     *
     * <p>使用场景：
     * <ul>
     *   <li>Tiered Storage 将数据分段存储（内存、本地磁盘、远程存储）</li>
     *   <li>消费者通过此消息告知需要的数据段</li>
     *   <li>生产者根据 segmentId 从对应存储层读取数据</li>
     * </ul>
     *
     * Message to notify producer about the id of required segment.
     */
    static class SegmentId extends NettyMessage {

        private static final byte ID = 11;

        /** 子分区索引 */
        final int subpartitionId;

        /** 需要的 Segment ID，必须 > 0 */
        final int segmentId;

        /** 接收者 InputChannel ID */
        final InputChannelID receiverId;

        SegmentId(int subpartitionId, int segmentId, InputChannelID receiverId) {
            this.subpartitionId = subpartitionId;
            checkArgument(segmentId > 0L, "The segmentId should be greater than 0");
            this.segmentId = segmentId;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator,
                                ID,
                                Integer.BYTES + Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(subpartitionId);
                result.writeInt(segmentId);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static SegmentId readFrom(ByteBuf buffer) {
            int subpartitionId = buffer.readInt();
            int segmentId = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new SegmentId(subpartitionId, segmentId, receiverId);
        }

        @Override
        public String toString() {
            return String.format("SegmentId(%s : %d)", receiverId, segmentId);
        }
    }

    // ------------------------------------------------------------------------

    void writeToChannel(
            ChannelOutboundInvoker out,
            ChannelPromise promise,
            ByteBufAllocator allocator,
            Consumer<ByteBuf> consumer,
            byte id,
            int length)
            throws IOException {

        ByteBuf byteBuf = null;
        try {
            byteBuf = allocateBuffer(allocator, id, length);
            consumer.accept(byteBuf);
            out.write(byteBuf, promise);
        } catch (Throwable t) {
            handleException(byteBuf, null, t);
        }
    }

    void handleException(@Nullable ByteBuf byteBuf, @Nullable Buffer buffer, Throwable t)
            throws IOException {
        if (byteBuf != null) {
            byteBuf.release();
        }
        if (buffer != null) {
            buffer.recycleBuffer();
        }
        ExceptionUtils.rethrowIOException(t);
    }
}
