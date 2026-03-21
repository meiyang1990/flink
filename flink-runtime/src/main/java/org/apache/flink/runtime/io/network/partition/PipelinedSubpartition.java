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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link ResultSubpartition#add(BufferConsumer)} adds a finished {@link BufferConsumer}
 * or a second {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via {@link
 * ResultSubpartition#createReadView(BufferAvailabilityListener)} of new data availability. Except
 * by calling {@link #flush()} explicitly, we always only notify when the first finished buffer
 * turns up and then, the reader has to drain the buffers via {@link #pollBuffer()} until its return
 * value shows no more buffers being available. This results in a buffer queue which is either empty
 * or has an unfinished {@link BufferConsumer} left from which the notifications will eventually
 * start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this {@link
 * PipelinedSubpartitionView#notifyDataAvailable() notification} for any {@link BufferConsumer}
 * present in the queue.
 *
 * <p>【学习笔记】PipelinedSubpartition 是 Flink 流处理的核心数据传输组件。
 *
 * <h3>一、核心特性</h3>
 * <ul>
 *   <li><b>流水线传输</b>：数据生产后立即可被消费，无需等待全部数据写完</li>
 *   <li><b>纯内存</b>：数据只存储在内存中，不落盘（除非背压严重导致溢出）</li>
 *   <li><b>单次消费</b>：每个子分区只能被一个下游 Task 消费一次</li>
 * </ul>
 *
 * <h3>二、数据通知机制</h3>
 * <p>采用"懒通知"策略以减少通知开销：
 * <ol>
 *   <li>只在第一个完成的 Buffer 到达时通知下游</li>
 *   <li>下游收到通知后，持续拉取直到队列为空</li>
 *   <li>显式调用 flush() 可强制触发通知</li>
 * </ol>
 *
 * <h3>三、与 Checkpoint 的交互</h3>
 * <ul>
 *   <li><b>优先级事件</b>：CheckpointBarrier 可以作为优先级事件插队到队首</li>
 *   <li><b>对齐超时</b>：对齐检查点超时后，转为非对齐检查点并记录 in-flight 数据</li>
 *   <li><b>阻塞/恢复</b>：Exactly-Once 语义下会暂停消费直到 Barrier 对齐</li>
 * </ul>
 *
 * <h3>四、背压机制</h3>
 * <ul>
 *   <li>通过 buffersInBacklog 反馈积压量</li>
 *   <li>下游基于 backlog 申请 Credit</li>
 *   <li>isBlocked 状态控制消费暂停/恢复</li>
 * </ul>
 */
public class PipelinedSubpartition extends ResultSubpartition implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

    // 默认优先级序列号，表示非优先级事件
    private static final int DEFAULT_PRIORITY_SEQUENCE_NUMBER = -1;

    // ------------------------------------------------------------------------

    /**
     * Number of exclusive credits per input channel at the downstream tasks.
     * 下游每个输入通道的独占 Credit 数量。
     * Credit-based 流控机制：下游持有 Credit 才能接收数据，0 表示无独占 Credit，
     * 此时下游完全依赖浮动 Credit，可能导致空 Buffer 也需要传输以释放 Credit。
     */
    private final int receiverExclusiveBuffersPerChannel;

    /**
     * All buffers of this subpartition. Access to the buffers is synchronized on this object.
     * 子分区的所有 Buffer 队列，使用优先级双端队列实现。
     * PrioritizedDeque 支持普通元素和优先级元素，优先级元素（如 CheckpointBarrier）可以插队到队首。
     */
    final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers =
            new PrioritizedDeque<>();

    /**
     * The number of non-event buffers currently in this subpartition.
     * 当前子分区中非事件 Buffer 的数量（即数据 Buffer）。
     * 这是 backlog 的核心指标，下游据此申请 Credit。
     */
    @GuardedBy("buffers")
    private int buffersInBacklog;

    /**
     * The read view to consume this subpartition.
     * 消费此子分区的读视图，每个子分区只能有一个读视图（单次消费语义）。
     */
    PipelinedSubpartitionView readView;

    /**
     * Flag indicating whether the subpartition has been finished.
     * 子分区是否已完成（上游 Task 写完所有数据）。
     */
    private boolean isFinished;

    /**
     * 是否有 flush 请求。flush 会强制通知下游数据可用，即使 Buffer 未完成。
     */
    @GuardedBy("buffers")
    private boolean flushRequested;

    /**
     * Flag indicating whether the subpartition has been released.
     * 子分区是否已释放。volatile 保证多线程可见性。
     */
    volatile boolean isReleased;

    /**
     * The total number of buffers (both data and event buffers).
     * 总 Buffer 数量（数据 + 事件），用于统计监控。
     */
    private long totalNumberOfBuffers;

    /**
     * The total number of bytes (both data and event buffers).
     * 总字节数，用于统计监控。
     */
    private long totalNumberOfBytes;

    /**
     * Writes in-flight data.
     * 用于写入 in-flight 数据的通道状态写入器，支持 Unaligned Checkpoint。
     */
    private ChannelStateWriter channelStateWriter;

    // 动态调整的 Buffer 大小
    private int bufferSize;

    /**
     * The channelState Future of unaligned checkpoint.
     * 非对齐检查点的通道状态 Future，用于异步收集 in-flight 数据。
     */
    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> channelStateFuture;

    /**
     * It is the checkpointId corresponding to channelStateFuture. And It should be always update
     * with {@link #channelStateFuture}.
     * 与 channelStateFuture 对应的检查点 ID，两者必须同步更新。
     */
    @GuardedBy("buffers")
    private long channelStateCheckpointId;

    /**
     * Whether this subpartition is blocked (e.g. by exactly once checkpoint) and is waiting for
     * resumption.
     * 子分区是否被阻塞（如 Exactly-Once 检查点对齐期间）。
     * 阻塞时下游无法消费数据，直到 Barrier 对齐后调用 resumeConsumption()。
     */
    @GuardedBy("buffers")
    boolean isBlocked = false;

    // 当前序列号，每发送一个 BufferAndBacklog 递增，用于保证数据顺序
    int sequenceNumber = 0;

    // ------------------------------------------------------------------------

    PipelinedSubpartition(
            int index,
            int receiverExclusiveBuffersPerChannel,
            int startingBufferSize,
            ResultPartition parent) {
        super(index, parent);

        checkArgument(
                receiverExclusiveBuffersPerChannel >= 0,
                "Buffers per channel must be non-negative.");
        this.receiverExclusiveBuffersPerChannel = receiverExclusiveBuffersPerChannel;
        this.bufferSize = startingBufferSize;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) {
        return add(bufferConsumer, partialRecordLength, false);
    }

    public boolean isSupportChannelStateRecover() {
        return true;
    }

    @Override
    public int finish() throws IOException {
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        add(eventBufferConsumer, 0, true);
        LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
        return eventBufferConsumer.getWrittenBytes();
    }

    private int add(BufferConsumer bufferConsumer, int partialRecordLength, boolean finish) {
        checkNotNull(bufferConsumer);

        final boolean notifyDataAvailable;
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        int newBufferSize;
        synchronized (buffers) {
            if (isFinished || isReleased) {
                bufferConsumer.close();
                return ADD_BUFFER_ERROR_CODE;
            }

            // Add the bufferConsumer and update the stats
            if (addBuffer(bufferConsumer, partialRecordLength)) {
                prioritySequenceNumber = sequenceNumber;
            }
            updateStatistics(bufferConsumer);
            increaseBuffersInBacklog(bufferConsumer);
            notifyDataAvailable = finish || shouldNotifyDataAvailable();

            isFinished |= finish;
            newBufferSize = bufferSize;
        }

        notifyPriorityEvent(prioritySequenceNumber);
        if (notifyDataAvailable) {
            notifyDataAvailable();
        }

        return newBufferSize;
    }

    @GuardedBy("buffers")
    private boolean addBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        assert Thread.holdsLock(buffers);
        if (bufferConsumer.getDataType().hasPriority()) {
            return processPriorityBuffer(bufferConsumer, partialRecordLength);
        } else if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                == bufferConsumer.getDataType()) {
            processTimeoutableCheckpointBarrier(bufferConsumer);
        }
        buffers.add(new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        return false;
    }

    @GuardedBy("buffers")
    private boolean processPriorityBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        final int numPriorityElements = buffers.getNumPriorityElements();

        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        if (barrier != null) {
            checkState(
                    barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                    "Only unaligned checkpoints should be priority events");
            final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
            Iterators.advance(iterator, numPriorityElements);
            List<Buffer> inflightBuffers = new ArrayList<>();
            while (iterator.hasNext()) {
                BufferConsumer buffer = iterator.next().getBufferConsumer();

                if (buffer.isBuffer()) {
                    try (BufferConsumer bc = buffer.copy()) {
                        inflightBuffers.add(bc.build());
                    }
                }
            }
            if (!inflightBuffers.isEmpty()) {
                channelStateWriter.addOutputData(
                        barrier.getId(),
                        subpartitionInfo,
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        inflightBuffers.toArray(new Buffer[0]));
            }
        }
        return needNotifyPriorityEvent();
    }

    // It is just called after add priorityEvent.
    @GuardedBy("buffers")
    private boolean needNotifyPriorityEvent() {
        assert Thread.holdsLock(buffers);
        // if subpartition is blocked then downstream doesn't expect any notifications
        return buffers.getNumPriorityElements() == 1 && !isBlocked;
    }

    @GuardedBy("buffers")
    private void processTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        channelStateWriter.addOutputDataFuture(
                barrier.getId(),
                subpartitionInfo,
                ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                createChannelStateFuture(barrier.getId()));
    }

    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> createChannelStateFuture(long checkpointId) {
        assert Thread.holdsLock(buffers);
        if (channelStateFuture != null) {
            completeChannelStateFuture(
                    null,
                    new IllegalStateException(
                            String.format(
                                    "%s has uncompleted channelStateFuture of checkpointId=%s, but it received "
                                            + "a new timeoutable checkpoint barrier of checkpointId=%s, it maybe "
                                            + "a bug due to currently not supported concurrent unaligned checkpoint.",
                                    this, channelStateCheckpointId, checkpointId)));
        }
        channelStateFuture = new CompletableFuture<>();
        channelStateCheckpointId = checkpointId;
        return channelStateFuture;
    }

    @GuardedBy("buffers")
    private void completeChannelStateFuture(List<Buffer> channelResult, Throwable e) {
        assert Thread.holdsLock(buffers);
        if (e != null) {
            channelStateFuture.completeExceptionally(e);
        } else {
            channelStateFuture.complete(channelResult);
        }
        channelStateFuture = null;
    }

    @GuardedBy("buffers")
    private boolean isChannelStateFutureAvailable(long checkpointId) {
        assert Thread.holdsLock(buffers);
        return channelStateFuture != null && channelStateCheckpointId == checkpointId;
    }

    private CheckpointBarrier parseAndCheckTimeoutableCheckpointBarrier(
            BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        checkArgument(barrier != null, "Parse the timeoutable Checkpoint Barrier failed.");
        checkState(
                barrier.getCheckpointOptions().isTimeoutable()
                        && Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                                == bufferConsumer.getDataType());
        return barrier;
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // 对齐检查点超时处理：将对齐检查点转换为非对齐检查点
        // 这是 Flink 1.11+ 引入的 Timeoutable Aligned Checkpoint 特性
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        synchronized (buffers) {
            // The checkpoint barrier has sent to downstream, so nothing to do.
            // 如果 Barrier 已经发送到下游，则无需处理
            if (!isChannelStateFutureAvailable(checkpointId)) {
                return;
            }

            // 1. find inflightBuffers and timeout the aligned barrier to unaligned barrier
            // 收集 Barrier 之前的 in-flight 数据，并将对齐 Barrier 转为非对齐 Barrier
            List<Buffer> inflightBuffers = new ArrayList<>();
            try {
                if (findInflightBuffersAndMakeBarrierToPriority(checkpointId, inflightBuffers)) {
                    prioritySequenceNumber = sequenceNumber;
                }
            } catch (IOException e) {
                inflightBuffers.forEach(Buffer::recycleBuffer);
                completeChannelStateFuture(null, e);
                throw e;
            }

            // 2. complete the channelStateFuture
            // 完成通道状态 Future，将 in-flight 数据传递给检查点协调器
            completeChannelStateFuture(inflightBuffers, null);
        }

        // 3. notify downstream read barrier, it must be called outside the buffers_lock to avoid
        // the deadlock.
        // 通知下游读取 Barrier，必须在锁外调用以避免死锁
        notifyPriorityEvent(prioritySequenceNumber);
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        synchronized (buffers) {
            if (isChannelStateFutureAvailable(checkpointId)) {
                completeChannelStateFuture(null, cause);
            }
        }
    }

    @GuardedBy("buffers")
    private boolean findInflightBuffersAndMakeBarrierToPriority(
            long checkpointId, List<Buffer> inflightBuffers) throws IOException {
        // 1. record the buffers before barrier as inflightBuffers
        final int numPriorityElements = buffers.getNumPriorityElements();
        final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
        Iterators.advance(iterator, numPriorityElements);

        BufferConsumerWithPartialRecordLength element = null;
        CheckpointBarrier barrier = null;
        while (iterator.hasNext()) {
            BufferConsumerWithPartialRecordLength next = iterator.next();
            BufferConsumer bufferConsumer = next.getBufferConsumer();

            if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                    == bufferConsumer.getDataType()) {
                barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
                // It may be an aborted barrier
                if (barrier.getId() != checkpointId) {
                    continue;
                }
                element = next;
                break;
            } else if (bufferConsumer.isBuffer()) {
                try (BufferConsumer bc = bufferConsumer.copy()) {
                    inflightBuffers.add(bc.build());
                }
            }
        }

        // 2. Make the barrier to be priority
        checkNotNull(
                element, "The checkpoint barrier=%d don't find in %s.", checkpointId, toString());
        makeBarrierToPriority(element, barrier);

        return needNotifyPriorityEvent();
    }

    private void makeBarrierToPriority(
            BufferConsumerWithPartialRecordLength oldElement, CheckpointBarrier barrier)
            throws IOException {
        buffers.getAndRemove(oldElement::equals);
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(
                        EventSerializer.toBufferConsumer(barrier.asUnaligned(), true), 0));
    }

    @Nullable
    private CheckpointBarrier parseCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier;
        try (BufferConsumer bc = bufferConsumer.copy()) {
            Buffer buffer = bc.build();
            try {
                final AbstractEvent event =
                        EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
                barrier = event instanceof CheckpointBarrier ? (CheckpointBarrier) event : null;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Should always be able to deserialize in-memory event", e);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return barrier;
    }

    @Override
    public void release() {
        // view reference accessible outside the lock, but assigned inside the locked scope
        final PipelinedSubpartitionView view;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            for (BufferConsumerWithPartialRecordLength buffer : buffers) {
                buffer.getBufferConsumer().close();
            }
            buffers.clear();

            if (channelStateFuture != null) {
                IllegalStateException exception =
                        new IllegalStateException("The PipelinedSubpartition is released");
                completeChannelStateFuture(null, exception);
            }

            view = readView;
            readView = null;

            // Make sure that no further buffers are added to the subpartition
            isReleased = true;
        }

        LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

        if (view != null) {
            view.releaseAllResources();
        }
    }

    /**
     * 从子分区中拉取一个 Buffer。
     *
     * <p>【核心流程】：
     * <ol>
     *   <li>检查是否被阻塞（Exactly-Once 检查点对齐期间）</li>
     *   <li>从队列中获取 Buffer 并构建 slice</li>
     *   <li>处理 Timeoutable Checkpoint Barrier</li>
     *   <li>更新统计信息和序列号</li>
     *   <li>如果遇到阻塞上游的数据类型，设置 isBlocked=true</li>
     * </ol>
     *
     * @return BufferAndBacklog 包含数据、积压量和下一个数据类型，返回 null 表示无数据可读
     */
    @Nullable
    BufferAndBacklog pollBuffer() {
        synchronized (buffers) {
            // 如果被阻塞（如 Exactly-Once 检查点对齐），不返回任何数据
            if (isBlocked) {
                return null;
            }

            Buffer buffer = null;

            if (buffers.isEmpty()) {
                flushRequested = false;
            }

            // 循环处理队列中的 Buffer
            while (!buffers.isEmpty()) {
                BufferConsumerWithPartialRecordLength bufferConsumerWithPartialRecordLength =
                        buffers.peek();
                BufferConsumer bufferConsumer =
                        bufferConsumerWithPartialRecordLength.getBufferConsumer();
                // 处理可超时的对齐检查点 Barrier
                if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                        == bufferConsumer.getDataType()) {
                    completeTimeoutableCheckpointBarrier(bufferConsumer);
                }
                buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);

                checkState(
                        bufferConsumer.isFinished() || buffers.size() == 1,
                        "When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

                if (buffers.size() == 1) {
                    // turn off flushRequested flag if we drained all the available data
                    // 如果队列中只剩一个 Buffer，关闭 flush 标志
                    flushRequested = false;
                }

                if (bufferConsumer.isFinished()) {
                    // Buffer 已完成，从队列移除并减少 backlog
                    requireNonNull(buffers.poll()).getBufferConsumer().close();
                    decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
                }

                // if we have an empty finished buffer and the exclusive credit is 0, we just return
                // the empty buffer so that the downstream task can release the allocated credit for
                // this empty buffer, this happens in two main scenarios currently:
                // 1. all data of a buffer builder has been read and after that the buffer builder
                // is finished
                // 2. in approximate recovery mode, a partial record takes a whole buffer builder
                // 空 Buffer 特殊处理：当独占 Credit 为 0 时，返回空 Buffer 以释放下游的 Credit
                if (receiverExclusiveBuffersPerChannel == 0 && bufferConsumer.isFinished()) {
                    break;
                }

                if (buffer.readableBytes() > 0) {
                    break;
                }
                buffer.recycleBuffer();
                buffer = null;
                if (!bufferConsumer.isFinished()) {
                    break;
                }
            }

            if (buffer == null) {
                return null;
            }

            // 如果遇到阻塞上游的数据类型（如 CheckpointBarrier），设置阻塞状态
            if (buffer.getDataType().isBlockingUpstream()) {
                isBlocked = true;
            }

            updateStatistics(buffer);
            // Do not report last remaining buffer on buffers as available to read (assuming it's
            // unfinished).
            // It will be reported for reading either on flush or when the number of buffers in the
            // queue
            // will be 2 or more.
            NetworkActionsLogger.traceOutput(
                    "PipelinedSubpartition#pollBuffer",
                    buffer,
                    parent.getOwningTaskName(),
                    subpartitionInfo);
            // 返回 Buffer 和背压信息
            return new BufferAndBacklog(
                    buffer,
                    getBuffersInBacklogUnsafe(),
                    isDataAvailableUnsafe() ? getNextBufferTypeUnsafe() : Buffer.DataType.NONE,
                    sequenceNumber++);
        }
    }

    @GuardedBy("buffers")
    private void completeTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        if (!isChannelStateFutureAvailable(barrier.getId())) {
            // It happens on a previously aborted checkpoint.
            return;
        }
        completeChannelStateFuture(Collections.emptyList(), null);
    }

    void resumeConsumption() {
        synchronized (buffers) {
            checkState(isBlocked, "Should be blocked by checkpoint.");

            isBlocked = false;
        }
    }

    public void acknowledgeAllDataProcessed() {
        parent.onSubpartitionAllDataProcessed(subpartitionInfo.getSubPartitionIdx());
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public PipelinedSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) {
        synchronized (buffers) {
            checkState(!isReleased);
            checkState(
                    readView == null,
                    "Subpartition %s of is being (or already has been) consumed, "
                            + "but pipelined subpartitions can only be consumed once.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            readView = new PipelinedSubpartitionView(this, availabilityListener);
        }

        return readView;
    }

    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            boolean isCreditAvailable) {
        synchronized (buffers) {
            boolean isAvailable;
            if (isCreditAvailable) {
                isAvailable = isDataAvailableUnsafe();
            } else {
                isAvailable = getNextBufferTypeUnsafe().isEvent();
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(
                    isAvailable, getBuffersInBacklogUnsafe());
        }
    }

    @GuardedBy("buffers")
    private boolean isDataAvailableUnsafe() {
        assert Thread.holdsLock(buffers);

        return !isBlocked && (flushRequested || getNumberOfFinishedBuffers() > 0);
    }

    private Buffer.DataType getNextBufferTypeUnsafe() {
        assert Thread.holdsLock(buffers);

        final BufferConsumerWithPartialRecordLength first = buffers.peek();
        return first != null ? first.getBufferConsumer().getDataType() : Buffer.DataType.NONE;
    }

    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        if (desirableNewBufferSize < 0) {
            throw new IllegalArgumentException("New buffer size can not be less than zero");
        }
        synchronized (buffers) {
            bufferSize = desirableNewBufferSize;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final long numBuffers;
        final long numBytes;
        final boolean finished;
        final boolean hasReadView;

        synchronized (buffers) {
            numBuffers = getTotalNumberOfBuffersUnsafe();
            numBytes = getTotalNumberOfBytesUnsafe();
            finished = isFinished;
            hasReadView = readView != null;
        }

        return String.format(
                "%s#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
                this.getClass().getSimpleName(),
                getSubPartitionIndex(),
                numBuffers,
                numBytes,
                getBuffersInBacklogUnsafe(),
                finished,
                hasReadView);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        // since we do not synchronize, the size may actually be lower than 0!
        return Math.max(buffers.size(), 0);
    }

    @Override
    public void flush() {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            if (buffers.isEmpty() || flushRequested) {
                return;
            }
            // if there is more than 1 buffer, we already notified the reader
            // (at the latest when adding the second buffer)
            boolean isDataAvailableInUnfinishedBuffer =
                    buffers.size() == 1 && buffers.peek().getBufferConsumer().isDataAvailable();
            notifyDataAvailable = !isBlocked && isDataAvailableInUnfinishedBuffer;
            flushRequested = buffers.size() > 1 || isDataAvailableInUnfinishedBuffer;
        }
        if (notifyDataAvailable) {
            notifyDataAvailable();
        }
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return totalNumberOfBuffers;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return totalNumberOfBytes;
    }

    Throwable getFailureCause() {
        return parent.getFailureCause();
    }

    private void updateStatistics(BufferConsumer buffer) {
        totalNumberOfBuffers++;
    }

    private void updateStatistics(Buffer buffer) {
        totalNumberOfBytes += buffer.getSize();
    }

    @GuardedBy("buffers")
    private void decreaseBuffersInBacklogUnsafe(boolean isBuffer) {
        assert Thread.holdsLock(buffers);
        if (isBuffer) {
            buffersInBacklog--;
        }
    }

    /**
     * Increases the number of non-event buffers by one after adding a non-event buffer into this
     * subpartition.
     */
    @GuardedBy("buffers")
    private void increaseBuffersInBacklog(BufferConsumer buffer) {
        assert Thread.holdsLock(buffers);

        if (buffer != null && buffer.isBuffer()) {
            buffersInBacklog++;
        }
    }

    /** Gets the number of non-event buffers in this subpartition. */
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int getBuffersInBacklogUnsafe() {
        if (isBlocked || buffers.isEmpty()) {
            return 0;
        }

        if (flushRequested
                || isFinished
                || !checkNotNull(buffers.peekLast()).getBufferConsumer().isBuffer()) {
            return buffersInBacklog;
        } else {
            return Math.max(buffersInBacklog - 1, 0);
        }
    }

    @GuardedBy("buffers")
    private boolean shouldNotifyDataAvailable() {
        // Notify only when we added first finished buffer.
        return readView != null
                && !flushRequested
                && !isBlocked
                && getNumberOfFinishedBuffers() == 1;
    }

    private void notifyDataAvailable() {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null) {
            readView.notifyDataAvailable();
        }
    }

    private void notifyPriorityEvent(int prioritySequenceNumber) {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null && prioritySequenceNumber != DEFAULT_PRIORITY_SEQUENCE_NUMBER) {
            readView.notifyPriorityEvent(prioritySequenceNumber);
        }
    }

    private int getNumberOfFinishedBuffers() {
        assert Thread.holdsLock(buffers);

        // NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
        // worst-case: a single finished buffer sits around until the next flush() call
        // (but we do not offer stronger guarantees anyway)
        final int numBuffers = buffers.size();
        if (numBuffers == 1 && buffers.peekLast().getBufferConsumer().isFinished()) {
            return 1;
        }

        // We assume that only last buffer is not finished.
        return Math.max(0, numBuffers - 1);
    }

    Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
        return buffer.build();
    }

    /** for testing only. */
    @VisibleForTesting
    BufferConsumerWithPartialRecordLength getNextBuffer() {
        return buffers.poll();
    }

    /** for testing only. */
    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    CompletableFuture<List<Buffer>> getChannelStateFuture() {
        return channelStateFuture;
    }

    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    public long getChannelStateCheckpointId() {
        return channelStateCheckpointId;
    }
}
