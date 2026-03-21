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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Subpartition data reader for {@link SortMergeResultPartition}.
 *
 * <p>Sort-Merge 子分区读取器，实现 {@link ResultSubpartitionView} 接口，负责从磁盘文件读取数据
 * 并提供给 Netty 层消费。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>异步读取</b>：数据由 {@link SortMergeResultPartitionReadScheduler} 的 I/O 线程异步读取</li>
 *   <li><b>优先级调度</b>：实现 Comparable 接口，基于文件偏移量和缓冲区数量进行优先级排序</li>
 *   <li><b>FullyFilledBuffer</b>：将多个小 buffer 合并成完整的逻辑 buffer，减少网络传输开销</li>
 *   <li><b>背压支持</b>：通过 dataBufferBacklog 跟踪积压数据量</li>
 * </ul>
 *
 * <h2>线程模型</h2>
 * <ul>
 *   <li><b>I/O 线程</b>：调用 readBuffers() 从文件读取数据到 buffersRead 队列</li>
 *   <li><b>Netty 线程</b>：调用 getNextBuffer() 消费 buffersRead 中的数据</li>
 *   <li>两个线程通过 lock 对象同步访问共享状态</li>
 * </ul>
 *
 * <h2>数据流程</h2>
 * <pre>
 * PartitionedFileReader.readCurrentRegion()
 *         ↓
 *    addBuffer() / addBufferToFullyFilledBuffer()
 *         ↓
 *    fullyFilledBuffersToRead → buffersRead
 *         ↓
 *    getNextBuffer() (Netty 消费)
 * </pre>
 *
 * @see SortMergeResultPartition
 * @see SortMergeResultPartitionReadScheduler
 */
class SortMergeSubpartitionReader
        implements ResultSubpartitionView, Comparable<SortMergeSubpartitionReader> {

    /** 保护共享状态访问的锁对象 */
    private final Object lock = new Object();

    /**
     * 释放完成的 Future，在 releaseInternal() 中完成。
     * 用于 ReadScheduler 等待所有 reader 释放后再清理资源。
     */
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /**
     * 数据可用性监听器，当有新数据可消费时通知 Netty 层。
     * 通常由 CreditBasedPartitionRequestClientHandler 实现。
     */
    private final BufferAvailabilityListener availabilityListener;

    /**
     * 已读取的缓冲区队列，可被 Netty 线程消费。
     * 由 I/O 线程填充，Netty 线程消费。
     */
    @GuardedBy("lock")
    private final Queue<Buffer> buffersRead = new ArrayDeque<>();

    /**
     * 分区文件读取器，负责从磁盘读取数据。
     * 维护当前读取位置和数据区域信息。
     */
    private final PartitionedFileReader fileReader;

    /**
     * 缓冲区队列中剩余的非事件 buffer 数量（即数据 buffer 数量）。
     * 用于 Credit-based 流量控制的背压计算。
     */
    @GuardedBy("lock")
    private int dataBufferBacklog;

    /** 该 reader 是否已释放 */
    @GuardedBy("lock")
    private boolean isReleased;

    /** 失败原因，将传播给消费端 Task */
    @GuardedBy("lock")
    private Throwable failureCause;

    /** 下一个发送给消费端的 buffer 的序列号，用于保证顺序性 */
    private int sequenceNumber;

    /**
     * 待转移到 buffersRead 的 FullyFilledBuffer 队列。
     * I/O 线程读取数据时先放入此队列，读取完成后批量转移到 buffersRead。
     */
    @GuardedBy("lock")
    private final Queue<FullyFilledBuffer> fullyFilledBuffersToRead = new ArrayDeque<>();

    /**
     * 当前正在填充的 FullyFilledBuffer。
     * 多个小 buffer 会被合并到同一个 FullyFilledBuffer 中，直到填满或遇到类型变化。
     */
    private FullyFilledBuffer toFilledBuffer;

    /** 页面大小，即单个 FullyFilledBuffer 的目标容量 */
    private final int pageSize;

    SortMergeSubpartitionReader(
            int pageSize, BufferAvailabilityListener listener, PartitionedFileReader fileReader) {
        this.availabilityListener = checkNotNull(listener);
        this.fileReader = checkNotNull(fileReader);
        this.pageSize = pageSize;
    }

    /**
     * 获取下一个可消费的 buffer。
     *
     * <p>由 Netty 线程调用，返回 BufferAndBacklog 包含：
     * <ul>
     *   <li>当前 buffer 数据</li>
     *   <li>下一个 buffer 的数据类型（用于预判是否有更多数据）</li>
     *   <li>当前数据积压量</li>
     *   <li>序列号</li>
     * </ul>
     *
     * @return 带有背压信息的 buffer，如果没有可用数据则返回 null
     */
    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        synchronized (lock) {
            Buffer buffer = buffersRead.poll();
            if (buffer == null) {
                return null;
            }

            if (buffer.isBuffer()) {
                --dataBufferBacklog;
            }

            Buffer lookAhead = buffersRead.peek();
            BufferAndBacklog bufferAndBacklog =
                    BufferAndBacklog.fromBufferAndLookahead(
                            buffer,
                            lookAhead == null ? Buffer.DataType.NONE : lookAhead.getDataType(),
                            dataBufferBacklog,
                            sequenceNumber);
            sequenceNumber += ((FullyFilledBuffer) buffer).getPartialBuffers().size();
            return bufferAndBacklog;
        }
    }

    private void addBuffer(Buffer buffer, int repeatCount) {
        boolean needRecycleBuffer = false;

        synchronized (lock) {
            if (isReleased) {
                needRecycleBuffer = true;
            } else {
                addBufferToFullyFilledBuffer(buffer, repeatCount);
            }
        }

        if (needRecycleBuffer) {
            buffer.recycleBuffer();
            throw new IllegalStateException("Subpartition reader has been already released.");
        }
    }

    private void addBufferToFullyFilledBuffer(Buffer buffer, int repeatCount) {
        for (int i = 0; i < repeatCount; i++) {
            addBufferToFullyFilledBuffer(buffer);
            buffer.retainBuffer();
        }
        buffer.recycleBuffer();
    }

    /**
     * 将 buffer 添加到当前的 FullyFilledBuffer 中。
     *
     * <p>合并规则：
     * <ul>
     *   <li>如果当前没有 FullyFilledBuffer，创建一个新的</li>
     *   <li>如果 buffer 无法放入当前 FullyFilledBuffer（空间不足或类型不匹配），创建新的</li>
     *   <li>只有数据类型的 buffer 会增加 backlog 计数</li>
     * </ul>
     */
    private void addBufferToFullyFilledBuffer(Buffer buffer) {
        if (toFilledBuffer == null) {
            toFilledBuffer =
                    new FullyFilledBuffer(buffer.getDataType(), pageSize, buffer.isCompressed());
            fullyFilledBuffersToRead.add(toFilledBuffer);
            if (buffer.isBuffer()) {
                ++dataBufferBacklog;
            }
        }

        if (toFilledBuffer.missingLength() < buffer.getSize()
                || toFilledBuffer.getDataType() != buffer.getDataType()
                || toFilledBuffer.isCompressed() != buffer.isCompressed()) {
            checkState(!toFilledBuffer.getPartialBuffers().isEmpty());

            toFilledBuffer =
                    new FullyFilledBuffer(buffer.getDataType(), pageSize, buffer.isCompressed());
            fullyFilledBuffersToRead.add(toFilledBuffer);
            if (buffer.isBuffer()) {
                ++dataBufferBacklog;
            }
        }

        toFilledBuffer.addPartialBuffer(buffer);
    }

    /**
     * 由 {@link SortMergeResultPartitionReadScheduler} 的 I/O 线程调用，从文件读取数据。
     *
     * <p>执行步骤：
     * <ol>
     *   <li>调用 fileReader.readCurrentRegion() 读取当前数据区域</li>
     *   <li>将读取的 buffer 通过 addBuffer 回调合并到 FullyFilledBuffer</li>
     *   <li>将 fullyFilledBuffersToRead 转移到 buffersRead</li>
     *   <li>如果有新数据可用，通知 Netty 层</li>
     * </ol>
     *
     * @param buffers 可用的内存段队列
     * @param recycler 缓冲区回收器
     * @return true 如果还有更多数据待读取
     */
    boolean readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException {
        boolean hasRemaining = fileReader.readCurrentRegion(buffers, recycler, this::addBuffer);

        boolean canNotify;
        synchronized (lock) {
            boolean emptyBefore = buffersRead.isEmpty();
            buffersRead.addAll(fullyFilledBuffersToRead);
            fullyFilledBuffersToRead.clear();
            toFilledBuffer = null;

            boolean notEmptyAfter = !buffersRead.isEmpty();
            canNotify = notEmptyAfter && emptyBefore;
        }

        // can not be locked!
        if (canNotify) {
            notifyDataAvailable();
        }

        return hasRemaining;
    }

    CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    void fail(Throwable throwable) {
        checkArgument(throwable != null, "Must be not null.");

        releaseInternal(throwable);
        // notify the netty thread which will propagate the error to the consumer task
        notifyDataAvailable();
    }

    @Override
    public void notifyDataAvailable() {
        availabilityListener.notifyDataAvailable(this);
    }

    /**
     * 比较两个 reader 的优先级，用于 ReadScheduler 的优先级队列调度。
     *
     * <p>优先级规则：
     * <ol>
     *   <li>优先调度缓冲区为空的 reader（避免饥饿）</li>
     *   <li>文件偏移量较小的 reader 优先（顺序读取优化磁盘 I/O）</li>
     * </ol>
     */
    @Override
    public int compareTo(SortMergeSubpartitionReader that) {
        int thisQueuedBuffers = unsynchronizedGetNumberOfQueuedBuffers();
        int thatQueuedBuffers = that.unsynchronizedGetNumberOfQueuedBuffers();
        if (thisQueuedBuffers != thatQueuedBuffers
                && (thisQueuedBuffers == 0 || thatQueuedBuffers == 0)) {
            return thisQueuedBuffers > thatQueuedBuffers ? 1 : -1;
        }

        long thisPriority = fileReader.getPriority();
        long thatPriority = that.fileReader.getPriority();

        if (thisPriority == thatPriority) {
            return 0;
        }
        return thisPriority > thatPriority ? 1 : -1;
    }

    @Override
    public void releaseAllResources() {
        releaseInternal(null);
    }

    /**
     * 内部释放方法，清理所有资源。
     *
     * <p>执行步骤：
     * <ol>
     *   <li>设置 isReleased 标志</li>
     *   <li>记录失败原因（如果有）</li>
     *   <li>收集所有待回收的 buffer</li>
     *   <li>在锁外部回收 buffer（避免死锁）</li>
     *   <li>完成 releaseFuture</li>
     * </ol>
     *
     * @param throwable 失败原因，正常释放时为 null
     */
    private void releaseInternal(@Nullable Throwable throwable) {
        List<Buffer> buffersToRecycle;
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            if (failureCause == null) {
                failureCause = throwable;
            }
            buffersRead.addAll(fullyFilledBuffersToRead);
            fullyFilledBuffersToRead.clear();
            toFilledBuffer = null;
            buffersToRecycle = new ArrayList<>(buffersRead);
            buffersRead.clear();
            dataBufferBacklog = 0;
        }
        buffersToRecycle.forEach(Buffer::recycleBuffer);
        buffersToRecycle.clear();

        releaseFuture.complete(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    /**
     * 获取数据可用性和积压量信息。
     *
     * <p>可用性判断规则：
     * <ul>
     *   <li>已释放 → 可用（消费端需要处理释放状态）</li>
     *   <li>缓冲区为空 → 不可用</li>
     *   <li>有 Credit 或下一个是事件 → 可用</li>
     * </ul>
     *
     * @param isCreditAvailable 消费端是否还有 Credit
     * @return 可用性和积压量信息
     */
    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
        synchronized (lock) {
            boolean isAvailable;
            if (isReleased) {
                isAvailable = true;
            } else if (buffersRead.isEmpty()) {
                isAvailable = false;
            } else {
                isAvailable = isCreditAvailable || !buffersRead.peek().isBuffer();
            }
            return new AvailabilityWithBacklog(isAvailable, dataBufferBacklog);
        }
    }

    // suppress warning as this method is only for unsafe purpose.
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return buffersRead.size();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return buffersRead.size();
        }
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {}

    @Override
    public int peekNextBufferSubpartitionId() {
        // because sort merge shuffle does not care about subpartition id, so just return -1
        return -1;
    }
}
