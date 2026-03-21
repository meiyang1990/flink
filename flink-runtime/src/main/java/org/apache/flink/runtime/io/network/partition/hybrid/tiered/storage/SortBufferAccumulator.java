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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferWithSubpartition;
import org.apache.flink.runtime.io.network.partition.DataBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The sort-based implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator}
 * receives the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>The {@link SortBufferAccumulator} can help use less buffers to accumulate data, which
 * decouples the buffer usage with the number of parallelism. The number of buffers used by the
 * {@link SortBufferAccumulator} will be numBuffers at most. Once the {@link DataBuffer} is full, or
 * switching from broadcast to non-broadcast(or vice versa), the buffer in the sort buffer will be
 * flushed to the tiers.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>SortBufferAccumulator 是 {@link BufferAccumulator} 的排序型实现，使用内部排序缓冲区
 * （{@link TieredStorageSortBuffer}）来累积数据。相比 Hash 型实现，它的缓冲区使用量与下游并行度解耦，
 * 可以用固定数量的缓冲区服务任意数量的子分区。
 *
 * <h2>工作流程</h2>
 *
 * <pre>
 *   receive(record)
 *        │
 *        ▼
 * ┌─────────────────────────────────┐
 * │ switchCurrentDataBufferIfNeeded │ ◄─ 广播/单播切换时刷新
 * └─────────────────────────────────┘
 *        │
 *        ▼
 * ┌─────────────────────────────────┐
 * │   currentDataBuffer.append()   │ ◄─ 向排序缓冲区写入记录
 * └─────────────────────────────────┘
 *        │
 *   ┌────┴────┐
 *   │  成功？  │
 *   └────┬────┘
 *    是  │    否
 *        │    │
 *        │    ▼
 *        │  ┌─────────────────────┐
 *        │  │ 缓冲区已满或记录超大 │
 *        │  └─────────────────────┘
 *        │         │
 *        │         ▼
 *        │  ┌─────────────────┐     ┌───────────────────┐
 *        │  │ flushDataBuffer │ 或 │ writeLargeRecord │
 *        │  └─────────────────┘     └───────────────────┘
 *        │         │
 *        └────┬────┘
 *             ▼
 *       完成本次写入
 * </pre>
 *
 * <h2>关键特性</h2>
 *
 * <ul>
 *   <li><b>缓冲区复用</b>: 写缓冲区和读缓冲区各占一半，读完后回收给写侧复用</li>
 *   <li><b>大记录处理</b>: 超过排序缓冲区容量的记录直接拆分成多个 Buffer 写出</li>
 *   <li><b>广播/单播切换</b>: 模式切换时强制刷新当前缓冲区，确保数据完整性</li>
 *   <li><b>部分记录控制</b>: 支持配置是否允许记录跨缓冲区拆分</li>
 * </ul>
 *
 * <h2>线程安全</h2>
 *
 * <p>仅由主线程访问，但内部使用锁来保护 freeSegments 和 currentDataBuffer 的一致性，
 * 因为缓冲区回收回调可能来自其他线程。
 */
public class SortBufferAccumulator implements BufferAccumulator {

    // ================================================================================
    //  配置参数
    // ================================================================================

    /** The number of the subpartitions. */
    // 下游子分区的数量，决定排序缓冲区需要管理的子分区索引范围
    private final int numSubpartitions;

    /** The total number of the buffers used by the {@link SortBufferAccumulator}. */
    // 累积器使用的缓冲区总数上限，一半用于写入，一半用于读取
    private final int numBuffers;

    /** The byte size of one single buffer. */
    // 单个缓冲区的字节大小，用于计算大记录需要拆分成多少个缓冲区
    private final int bufferSizeBytes;

    // ================================================================================
    //  缓冲区管理
    // ================================================================================

    /** The empty buffers without storing data. */
    // 空闲内存段队列，排序缓冲区释放后的 MemorySegment 会被回收到这里
    @GuardedBy("lock")
    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    /** The memory manager of the tiered storage. */
    // 分层存储内存管理器，负责从全局缓冲池申请缓冲区
    private final TieredStorageMemoryManager memoryManager;

    // 是否允许记录跨缓冲区拆分（部分记录），影响读取时的 DataType 标记
    private final boolean isPartialRecordAllowed;

    // ================================================================================
    //  运行时状态
    // ================================================================================

    /**
     * The {@link DataBuffer} is utilized to accumulate the incoming records. Whenever there is a
     * transition from broadcast to non-broadcast (or vice versa), the buffer is flushed to ensure
     * data integrity. Note that this can be null before using it to store records, and this {@link
     * DataBuffer} will be released once flushed.
     */
    // 当前排序缓冲区，负责按子分区对记录进行排序和累积
    // 广播/单播切换或缓冲区写满时会刷新并重建
    @GuardedBy("lock")
    @Nullable
    private TieredStorageSortBuffer currentDataBuffer;

    /**
     * The buffer recycler. Note that this can be null before requesting buffers from the memory
     * manager.
     */
    // 缓冲区回收器，将用完的 MemorySegment 还给内存管理器
    @Nullable private BufferRecycler bufferRecycler;

    /**
     * The {@link SortBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    // 缓冲区刷新回调，将完成的缓冲区传递给下游 Tier 层
    // 参数: (子分区ID, 缓冲区数据, 后续连续缓冲区数量)
    @Nullable
    private TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> accumulatedBufferFlusher;

    /** Whether the current {@link DataBuffer} is a broadcast sort buffer. */
    // 当前排序缓冲区是否用于广播数据（发送给所有子分区）
    private boolean isBroadcastDataBuffer;

    // 当前排序缓冲区是否已释放，用于控制回收逻辑
    @GuardedBy("lock")
    private boolean isDataBufferReleased;

    // 同步锁，保护 freeSegments 和 currentDataBuffer 的并发访问
    private final Object lock = new Object();

    public SortBufferAccumulator(
            int numSubpartitions,
            int numBuffers,
            int bufferSizeBytes,
            TieredStorageMemoryManager memoryManager,
            boolean isPartialRecordAllowed) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSizeBytes = bufferSizeBytes;
        this.numBuffers = numBuffers;
        this.memoryManager = memoryManager;
        this.isPartialRecordAllowed = isPartialRecordAllowed;
    }

    @Override
    public void setup(TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> bufferFlusher) {
        this.accumulatedBufferFlusher = bufferFlusher;
    }

    /**
     * 接收并处理来自 TieredStorageProducerClient 的记录。
     *
     * <p>处理流程：
     * <ol>
     *   <li>检查是否需要切换排序缓冲区（广播/单播模式变更）</li>
     *   <li>尝试将记录追加到当前排序缓冲区</li>
     *   <li>如果追加失败且缓冲区为空，说明记录过大，直接写大记录</li>
     *   <li>如果追加失败且缓冲区有数据，刷新后递归重试</li>
     * </ol>
     */
    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        int targetSubpartition = subpartitionId.getSubpartitionId();
        synchronized (lock) {
            // 检查并执行广播/单播模式切换
            switchCurrentDataBufferIfNeeded(isBroadcast);
            // 尝试向排序缓冲区追加记录，返回 true 表示追加失败（缓冲区已满）
            if (!checkNotNull(currentDataBuffer).append(record, targetSubpartition, dataType)) {
                return;
            }

            // The sort buffer is empty, but we failed to write the record into it, which indicates
            // the record is larger than the sort buffer can hold. So the record is written into
            // multiple buffers directly.
            // 排序缓冲区为空但写入失败，说明单条记录超过缓冲区容量，需要特殊处理
            if (!currentDataBuffer.hasRemaining()) {
                isDataBufferReleased = true;
                currentDataBuffer.release();
                // 大记录直接拆分成多个 Buffer 写出
                writeLargeRecord(record, targetSubpartition, dataType);
                return;
            }
            // 缓冲区有数据但已满，先刷新现有数据
            flushDataBuffer();
        }

        checkState(record.hasRemaining(), "Empty record.");
        // 递归重试，继续写入剩余的记录数据
        receive(record, subpartitionId, dataType, isBroadcast);
    }

    /**
     * 关闭累积器，刷新所有未完成的数据并释放资源。
     */
    @Override
    public void close() {
        synchronized (lock) {
            // 刷新当前排序缓冲区中所有已累积的数据
            flushCurrentDataBuffer();
            isDataBufferReleased = true;
            // 将所有空闲 MemorySegment 归还给内存管理器
            releaseFreeBuffers();
            if (currentDataBuffer != null) {
                currentDataBuffer.release();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /**
     * 检查是否需要切换排序缓冲区（广播/单播模式变更时触发）。
     *
     * <p>以下情况需要切换：
     * <ul>
     *   <li>广播模式发生变化</li>
     *   <li>当前缓冲区为空或已释放或已完成</li>
     * </ul>
     */
    @GuardedBy("lock")
    private void switchCurrentDataBufferIfNeeded(boolean isBroadcast) {
        // 如果模式相同且缓冲区可用，无需切换
        if (isBroadcast == isBroadcastDataBuffer
                && currentDataBuffer != null
                && !currentDataBuffer.isReleased()
                && !currentDataBuffer.isFinished()) {
            return;
        }
        // 更新模式并刷新旧缓冲区，创建新缓冲区
        isBroadcastDataBuffer = isBroadcast;
        flushCurrentDataBuffer();
        isDataBufferReleased = true;
        currentDataBuffer = createNewDataBuffer();
        isDataBufferReleased = false;
    }

    /**
     * 创建新的排序缓冲区。
     *
     * <p>缓冲区分配策略：将空闲 MemorySegment 一分为二，
     * 一半用于写入数据（numBuffersForSort），一半用于读取输出。
     */
    @GuardedBy("lock")
    private TieredStorageSortBuffer createNewDataBuffer() {
        // 确保有足够的空闲缓冲区
        requestBuffers();

        // Use the half of the buffers for writing, and the other half for reading
        // 写入缓冲区数量 = 空闲缓冲区总数 / 2
        int numBuffersForSort = freeSegments.size() / 2;
        return new TieredStorageSortBuffer(
                freeSegments,
                this::recycleBuffer,
                numSubpartitions,
                bufferSizeBytes,
                numBuffersForSort,
                isPartialRecordAllowed);
    }

    /**
     * 从内存管理器申请缓冲区直到达到配额。
     */
    @GuardedBy("lock")
    private void requestBuffers() {
        while (freeSegments.size() < numBuffers) {
            Buffer buffer = requestBuffer();
            freeSegments.add(checkNotNull(buffer).getMemorySegment());
            // 首次申请时保存回收器引用
            if (bufferRecycler == null) {
                bufferRecycler = buffer.getRecycler();
            }
        }
    }

    /**
     * 刷新当前排序缓冲区中的所有数据到下游 Tier 层。
     *
     * <p>流程：
     * <ol>
     *   <li>调用 finish() 标记缓冲区写入完成，准备读取</li>
     *   <li>循环调用 getNextBuffer() 获取排序后的数据</li>
     *   <li>将每个缓冲区通过 accumulatedBufferFlusher 传递给下游</li>
     *   <li>释放排序缓冲区和空闲 MemorySegment</li>
     * </ol>
     */
    @GuardedBy("lock")
    private void flushDataBuffer() {
        if (currentDataBuffer == null
                || currentDataBuffer.isReleased()
                || !currentDataBuffer.hasRemaining()) {
            return;
        }
        // 完成写入阶段，准备读取
        currentDataBuffer.finish();

        do {
            MemorySegment freeSegment = getFreeSegment();
            // 从排序缓冲区获取下一个已排序的缓冲区
            BufferWithSubpartition bufferWithSubpartition =
                    currentDataBuffer.getNextBuffer(freeSegment);
            if (bufferWithSubpartition == null) {
                break;
            }
            // 计算当前记录还剩余多少个后续缓冲区（用于流量控制）
            int numRemainingConsecutiveBuffers =
                    (int)
                            Math.ceil(
                                    ((double) currentDataBuffer.getRecordRemainingBytes())
                                            / bufferSizeBytes);
            // 将缓冲区传递给下游
            flushBuffer(bufferWithSubpartition, numRemainingConsecutiveBuffers);
        } while (true);

        isDataBufferReleased = true;
        releaseFreeBuffers();
        currentDataBuffer.release();
    }

    /**
     * 刷新并释放当前排序缓冲区。
     */
    private void flushCurrentDataBuffer() {
        synchronized (lock) {
            if (currentDataBuffer != null) {
                flushDataBuffer();
                currentDataBuffer = null;
            }
        }
    }

    /**
     * 处理超大记录：将单条记录拆分成多个缓冲区直接写出。
     *
     * <p>当记录大小超过排序缓冲区容量时调用此方法，绕过排序直接输出。
     *
     * @param record           待写入的记录数据
     * @param subpartitionId   目标子分区索引
     * @param dataType         数据类型（不能是事件类型）
     */
    private void writeLargeRecord(ByteBuffer record, int subpartitionId, Buffer.DataType dataType) {
        // 大记录处理不支持事件类型
        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            // 每次写入一个缓冲区大小的数据
            int toCopy = Math.min(record.remaining(), bufferSizeBytes);
            MemorySegment writeBuffer = requestBuffer().getMemorySegment();
            writeBuffer.put(0, record, toCopy);

            // 计算后续还需要多少个缓冲区
            int numRemainingConsecutiveBuffers =
                    (int) Math.ceil(((double) record.remaining()) / bufferSizeBytes);
            // 最后一个缓冲区标记为 CLEAR_END，表示记录边界清晰
            if (numRemainingConsecutiveBuffers == 0) {
                dataType = Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END;
            }

            flushBuffer(
                    new BufferWithSubpartition(
                            new NetworkBuffer(
                                    writeBuffer, checkNotNull(bufferRecycler), dataType, toCopy),
                            subpartitionId),
                    numRemainingConsecutiveBuffers);
        }

        releaseFreeBuffers();
    }

    /**
     * 获取一个空闲 MemorySegment，如果队列为空则从内存管理器申请。
     */
    private MemorySegment getFreeSegment() {
        synchronized (lock) {
            MemorySegment freeSegment = freeSegments.poll();
            if (freeSegment == null) {
                freeSegment = requestBuffer().getMemorySegment();
            }
            return freeSegment;
        }
    }

    /**
     * 将完成的缓冲区传递给下游 Tier 层。
     *
     * @param bufferWithSubpartition      缓冲区及其目标子分区
     * @param numRemainingConsecutiveBuffers 当前记录还剩余的后续缓冲区数量
     */
    private void flushBuffer(
            BufferWithSubpartition bufferWithSubpartition, int numRemainingConsecutiveBuffers) {
        checkNotNull(accumulatedBufferFlusher)
                .accept(
                        new TieredStorageSubpartitionId(
                                bufferWithSubpartition.getSubpartitionIndex()),
                        bufferWithSubpartition.getBuffer(),
                        numRemainingConsecutiveBuffers);
    }

    /**
     * 从内存管理器申请单个缓冲区（阻塞操作）。
     *
     * <p>使用 BufferBuilder -> BufferConsumer -> Buffer 的转换链获取可直接使用的缓冲区。
     */
    private Buffer requestBuffer() {
        BufferBuilder bufferBuilder = memoryManager.requestBufferBlocking(this);
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        bufferBuilder.close();
        bufferConsumer.close();
        return buffer;
    }

    /**
     * 释放所有空闲缓冲区，归还给内存管理器。
     */
    private void releaseFreeBuffers() {
        synchronized (lock) {
            isDataBufferReleased = true;
            freeSegments.forEach(this::recycleBuffer);
            freeSegments.clear();
        }
    }

    /**
     * 回收单个 MemorySegment。
     *
     * <p>如果排序缓冲区仍在使用中，则放回空闲队列供复用；
     * 否则直接归还给全局内存池。
     */
    private void recycleBuffer(MemorySegment memorySegment) {
        synchronized (lock) {
            // 如果排序缓冲区还在使用，回收到本地空闲队列
            if (!isDataBufferReleased
                    && currentDataBuffer != null
                    && !currentDataBuffer.isReleased()) {
                freeSegments.add(memorySegment);
            } else {
                // 否则归还给全局内存池
                checkNotNull(bufferRecycler).recycle(memorySegment);
            }
        }
    }
}
