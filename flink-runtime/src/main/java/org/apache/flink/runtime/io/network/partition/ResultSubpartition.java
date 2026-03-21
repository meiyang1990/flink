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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A single subpartition of a {@link ResultPartition} instance.
 *
 * <p>【学习笔记】ResultSubpartition 是 Flink 网络层数据分发的核心抽象，代表 ResultPartition 中的一个子分区。
 *
 * <h3>一、核心概念</h3>
 * <ul>
 *   <li><b>子分区</b>：每个 ResultPartition 按下游消费者数量划分为多个子分区，每个子分区对应一个下游 Task</li>
 *   <li><b>数据缓冲</b>：子分区内部维护 Buffer 队列，存储待发送的数据和事件</li>
 *   <li><b>背压传导</b>：通过 backlog（积压量）指标向上游反馈消费速度，实现端到端背压</li>
 * </ul>
 *
 * <h3>二、主要实现类</h3>
 * <ul>
 *   <li>{@code PipelinedSubpartition}：流处理场景，内存中流水线式传输，数据生产后立即可消费</li>
 *   <li>{@code BoundedBlockingSubpartition}：批处理场景，阻塞式传输，数据全部写完后才能消费</li>
 * </ul>
 *
 * <h3>三、生命周期</h3>
 * <ol>
 *   <li><b>add()</b>：上游 Task 向子分区写入数据</li>
 *   <li><b>createReadView()</b>：下游 Task 创建读视图开始消费</li>
 *   <li><b>finish()</b>：上游 Task 写完所有数据</li>
 *   <li><b>release()</b>：释放子分区资源</li>
 * </ol>
 *
 * <h3>四、与 Checkpoint 的交互</h3>
 * <ul>
 *   <li>{@code alignedBarrierTimeout}：对齐检查点超时后转为非对齐检查点</li>
 *   <li>{@code abortCheckpoint}：取消检查点时清理相关状态</li>
 * </ul>
 */
public abstract class ResultSubpartition {

    // The error code when adding a buffer fails.
    // 添加 Buffer 失败时返回的错误码，下游可据此判断是否需要重试或关闭通道
    public static final int ADD_BUFFER_ERROR_CODE = -1;

    /** The info of the subpartition to identify it globally within a task. */
    // 子分区信息，包含分区索引和子分区索引，用于在整个作业中唯一标识该子分区
    protected final ResultSubpartitionInfo subpartitionInfo;

    /** The parent partition this subpartition belongs to. */
    // 所属的父 ResultPartition，提供分区级配置和状态信息
    protected final ResultPartition parent;

    // - Statistics ----------------------------------------------------------

    public ResultSubpartition(int index, ResultPartition parent) {
        this.parent = parent;
        this.subpartitionInfo = new ResultSubpartitionInfo(parent.getPartitionIndex(), index);
    }

    public ResultSubpartitionInfo getSubpartitionInfo() {
        return subpartitionInfo;
    }

    /** Gets the total numbers of buffers (data buffers plus events). */
    protected abstract long getTotalNumberOfBuffersUnsafe();

    protected abstract long getTotalNumberOfBytesUnsafe();

    public int getSubPartitionIndex() {
        return subpartitionInfo.getSubPartitionIdx();
    }

    /** Notifies the parent partition about a consumed {@link ResultSubpartitionView}. */
    protected void onConsumedSubpartition() {
        parent.onConsumedSubpartition(getSubPartitionIndex());
    }

    public abstract void alignedBarrierTimeout(long checkpointId) throws IOException;

    public abstract void abortCheckpoint(long checkpointId, CheckpointException cause);

    @VisibleForTesting
    public final int add(BufferConsumer bufferConsumer) throws IOException {
        return add(bufferConsumer, 0);
    }

    /**
     * Adds the given buffer.
     *
     * <p>The request may be executed synchronously, or asynchronously, depending on the
     * implementation.
     *
     * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must
     * be in finished state. Because of the performance reasons, this is only enforced during the
     * data reading. Priority events can be added while the previous buffer consumer is still open,
     * in which case the open buffer consumer is overtaken.
     *
     * @param bufferConsumer the buffer to add (transferring ownership to this writer)
     * @param partialRecordLength the length of bytes to skip in order to start with a complete
     *     record, from position index 0 of the underlying {@cite MemorySegment}.
     * @return the preferable buffer size for this subpartition or {@link #ADD_BUFFER_ERROR_CODE} if
     *     the add operation fails.
     * @throws IOException thrown in case of errors while adding the buffer
     */
    public abstract int add(BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException;

    public abstract void flush();

    /**
     * Writing of data is finished.
     *
     * @return the size of data written for this subpartition inside of finish.
     */
    public abstract int finish() throws IOException;

    public abstract void release() throws IOException;

    public abstract ResultSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) throws IOException;

    public abstract boolean isReleased();

    /** Gets the number of non-event buffers in this subpartition. */
    abstract int getBuffersInBacklogUnsafe();

    /**
     * Makes a best effort to get the current size of the queue. This method must not acquire locks
     * or interfere with the task and network threads in any way.
     */
    public abstract int unsynchronizedGetNumberOfQueuedBuffers();

    /** Get the current size of the queue. */
    public abstract int getNumberOfQueuedBuffers();

    public abstract void bufferSize(int desirableNewBufferSize);

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and the backlog length indicating how many non-event
     * buffers are available in the subpartition.
     *
     * <p>【学习笔记】BufferAndBacklog 是数据传输的核心数据结构：
     * <ul>
     *   <li><b>buffer</b>：实际的数据内容</li>
     *   <li><b>buffersInBacklog</b>：积压的数据 Buffer 数量，用于 Credit-based 流控</li>
     *   <li><b>nextDataType</b>：下一个数据的类型（数据/事件/无），便于消费者预判</li>
     *   <li><b>sequenceNumber</b>：序列号，用于保证数据顺序和重传检测</li>
     * </ul>
     */
    public static final class BufferAndBacklog {
        // 实际的数据 Buffer
        private final Buffer buffer;
        // 积压的非事件 Buffer 数量，下游据此申请 Credit
        private final int buffersInBacklog;
        // 下一个数据的类型，用于提前通知下游是否有数据/事件可读
        private final Buffer.DataType nextDataType;
        // 序列号，保证数据按序传输
        private final int sequenceNumber;

        public BufferAndBacklog(
                Buffer buffer,
                int buffersInBacklog,
                Buffer.DataType nextDataType,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.buffersInBacklog = buffersInBacklog;
            this.nextDataType = checkNotNull(nextDataType);
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean isDataAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean isEventAvailable() {
            return nextDataType.isEvent();
        }

        public Buffer.DataType getNextDataType() {
            return nextDataType;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public static BufferAndBacklog fromBufferAndLookahead(
                Buffer current, Buffer.DataType nextDataType, int backlog, int sequenceNumber) {
            return new BufferAndBacklog(current, backlog, nextDataType, sequenceNumber);
        }
    }
}
