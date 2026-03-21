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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * * A {@link DataBuffer} implementation which sorts all appended records only by subpartition
 * index. Records of the same subpartition keep the appended order.
 *
 * <p>Different from the {@link SortBasedDataBuffer}, in this {@link DataBuffer} implementation,
 * memory segment boundary serves as the nature data boundary of different subpartitions, which
 * means that one memory segment can never contain data from different subpartitions.
 *
 * <h2>核心设计概述</h2>
 * <p>HashBasedDataBuffer 是 {@link DataBuffer} 的一种实现，采用"分桶"策略组织数据，
 * 每个子分区独占一个或多个内存段（MemorySegment），不同子分区的数据物理上完全隔离。
 *
 * <h3>1. 数据组织方式 - 与 SortBasedDataBuffer 的核心区别</h3>
 * <pre>
 *   HashBasedDataBuffer 内存布局：
 *   +------------------+   +------------------+   +------------------+
 *   | Subpartition 0   |   | Subpartition 0   |   | Subpartition 1   |
 *   | Buffer 1         |   | Buffer 2         |   | Buffer 1         |
 *   | [Record][Record] |   | [Record][...]    |   | [Record][Record] |
 *   +------------------+   +------------------+   +------------------+
 *            ↓                      ↓                      ↓
 *         单个子分区可跨多个 Buffer，但每个 Buffer 只属于一个子分区
 *
 *   SortBasedDataBuffer 内存布局：
 *   +------------------+   +------------------+
 *   | [Index][Data]    |   | [Index][Data]    |
 *   | [SP0][SP1][SP0]  |   | [SP2][SP1][...]  |  ← 不同子分区数据混合存储
 *   +------------------+   +------------------+
 * </pre>
 *
 * <h3>2. 关键特性</h3>
 * <ul>
 *   <li><b>自然边界隔离</b>：内存段边界即子分区边界，读取时无需额外排序</li>
 *   <li><b>事件单独分配</b>：事件（如 Checkpoint Barrier）使用堆外内存单独存储</li>
 *   <li><b>部分写入支持</b>：当 Buffer 满时可以部分写入记录，剩余部分在下一个 Buffer 继续</li>
 *   <li><b>内存效率</b>：可能存在内部碎片（每个 Buffer 末尾的未使用空间）</li>
 * </ul>
 *
 * <h3>3. 使用场景</h3>
 * <ul>
 *   <li>子分区数量较少时（减少内存碎片）</li>
 *   <li>数据分布相对均匀时</li>
 *   <li>{@link SortMergeResultPartition} 的 unicast 数据缓冲</li>
 * </ul>
 *
 * <h3>4. 线程安全</h3>
 * <p>非线程安全，由调用者保证单线程访问。
 *
 * @see SortBasedDataBuffer
 * @see SortMergeResultPartition
 */
public class HashBasedDataBuffer implements DataBuffer {

    // =============================================================================================
    // 内存管理相关字段
    // =============================================================================================

    /**
     * A list of {@link MemorySegment}s used to store data in memory.
     *
     * <p>空闲内存段池，用于分配给各子分区写入数据。从 BufferPool 申请并由 bufferRecycler 回收。
     */
    private final LinkedList<MemorySegment> freeSegments;

    /**
     * {@link BufferRecycler} used to recycle {@link #freeSegments}.
     *
     * <p>缓冲区回收器，用于将使用完毕的内存段归还给内存池。
     */
    private final BufferRecycler bufferRecycler;

    /**
     * Number of guaranteed buffers can be allocated from the buffer pool for data sort.
     *
     * <p>保证可分配的缓冲区数量上限，用于容量控制和流控。
     */
    private final int numGuaranteedBuffers;

    /**
     * Buffers containing data for all subpartitions.
     *
     * <p>每个子分区对应一个 BufferConsumer 队列，存储该子分区的所有数据缓冲区。
     * 数组索引即子分区索引，实现 O(1) 时间复杂度的子分区定位。
     */
    private final ArrayDeque<BufferConsumer>[] buffers;

    /**
     * Size of buffers requested from buffer pool. All buffers must be of the same size.
     *
     * <p>统一的缓冲区大小（字节），所有内存段必须具有相同大小。
     */
    private final int bufferSize;

    // =============================================================================================
    // 统计信息和状态标志
    // =============================================================================================

    /**
     * Total number of bytes already appended to this sort buffer.
     *
     * <p>已写入的总字节数，用于统计和判断缓冲区使用情况。
     */
    private long numTotalBytes;

    /**
     * Total number of records already appended to this sort buffer.
     *
     * <p>已写入的记录总数（不含事件），用于监控和统计。
     */
    private long numTotalRecords;

    /**
     * Whether this sort buffer is finished. One can only read a finished sort buffer.
     *
     * <p>是否已完成写入。为 true 时表示进入只读状态，不能再追加数据。
     */
    private boolean isFinished;

    /**
     * Whether this sort buffer is released. A released sort buffer can not be used.
     *
     * <p>是否已释放。为 true 时表示资源已回收，不能再进行任何操作。
     */
    private boolean isReleased;

    // =============================================================================================
    // 写入相关字段
    // =============================================================================================

    /**
     * Partial buffers to be appended data for each subpartition.
     *
     * <p>每个子分区当前正在写入的 BufferBuilder。当 Builder 写满后会被 finish 并置为 null，
     * 下次写入时再创建新的 Builder。数组索引即子分区索引。
     */
    private final BufferBuilder[] builders;

    /**
     * Total number of network buffers already occupied currently by this sort buffer.
     *
     * <p>当前已占用的网络缓冲区数量，用于容量控制。不包括事件使用的堆外内存。
     */
    private int numBuffersOccupied;

    // =============================================================================================
    // 读取相关字段
    // =============================================================================================

    /**
     * Used to index the current available subpartition to read data from.
     *
     * <p>当前正在读取的子分区在读取顺序数组中的索引位置。
     */
    private int readOrderIndex;

    /**
     * Data of different subpartitions in this sort buffer will be read in this order.
     *
     * <p>子分区读取顺序数组，决定各子分区数据的输出顺序。
     * 默认按子分区索引顺序 [0, 1, 2, ...]，也可以通过构造函数自定义顺序。
     */
    private final int[] subpartitionReadOrder;

    /**
     * Total number of bytes already read from this sort buffer.
     *
     * <p>已读取的字节总数，用于判断是否还有剩余数据（与 numTotalBytes 对比）。
     */
    private long numTotalBytesRead;

    public HashBasedDataBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSize = bufferSize;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        checkState(numGuaranteedBuffers <= freeSegments.size(), "Wrong number of free segments.");

        this.builders = new BufferBuilder[numSubpartitions];
        this.buffers = new ArrayDeque[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            this.buffers[subpartition] = new ArrayDeque<>();
        }

        this.subpartitionReadOrder = new int[numSubpartitions];
        if (customReadOrder != null) {
            checkArgument(customReadOrder.length == numSubpartitions, "Illegal data read order.");
            System.arraycopy(customReadOrder, 0, this.subpartitionReadOrder, 0, numSubpartitions);
        } else {
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                this.subpartitionReadOrder[subpartition] = subpartition;
            }
        }
    }

    /**
     * Partial data of the target record can be written if this {@link HashBasedDataBuffer} is full.
     * The remaining data of the target record will be written to the next data region (a new data
     * buffer or this data buffer after reset).
     *
     * <p>追加数据到指定子分区。与 SortBasedDataBuffer 不同，本实现支持部分写入：
     * 当缓冲区满时可能只写入部分数据，剩余数据由调用者在下一个 DataBuffer 中继续写入。
     *
     * <p>处理流程：
     * <ol>
     *   <li>区分数据类型：记录（Buffer）走 writeRecord，事件走 writeEvent</li>
     *   <li>事件使用堆外内存单独存储，不占用 BufferPool 配额</li>
     *   <li>记录写入可能跨越多个 Buffer，支持部分写入</li>
     * </ol>
     */
    @Override
    public boolean append(ByteBuffer source, int targetSubpartition, Buffer.DataType dataType)
            throws IOException {
        checkArgument(source.hasRemaining(), "Cannot append empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();
        if (dataType.isBuffer()) {
            writeRecord(source, targetSubpartition);
        } else {
            writeEvent(source, targetSubpartition, dataType);
        }

        if (source.hasRemaining()) {
            return true;
        }
        ++numTotalRecords;
        numTotalBytes += totalBytes - source.remaining();
        return false;
    }

    /**
     * 写入事件数据（如 Checkpoint Barrier、EndOfPartition 等）。
     *
     * <p>事件处理的特殊性：
     * <ul>
     *   <li>事件使用堆外内存（UnpooledOffHeapMemory）单独分配，不占用 BufferPool 配额</li>
     *   <li>写入前会先 finish 当前子分区的 Builder，确保事件边界清晰</li>
     *   <li>事件数据直接封装为 BufferConsumer 加入队列</li>
     * </ul>
     */
    private void writeEvent(ByteBuffer source, int targetSubpartition, Buffer.DataType dataType) {
        BufferBuilder builder = builders[targetSubpartition];
        // 如果当前子分区有未完成的 Builder，先将其完成以确保事件边界
        if (builder != null) {
            builder.finish();
            builder.close();
            builders[targetSubpartition] = null;
        }

        // 为事件分配独立的堆外内存，避免占用 BufferPool 配额
        MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(source.remaining());
        segment.put(0, source, segment.size());
        BufferConsumer consumer =
                new BufferConsumer(
                        new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType),
                        segment.size());
        buffers[targetSubpartition].add(consumer);
    }

    /**
     * 写入普通记录数据。
     *
     * <p>写入流程：
     * <ol>
     *   <li>容量检查：判断剩余空间是否足够写入整条记录</li>
     *   <li>循环写入：记录可能跨越多个 Buffer，逐个填充直到写完</li>
     *   <li>Buffer 管理：当前 Buffer 满后自动 finish 并创建新 Buffer</li>
     * </ol>
     */
    private void writeRecord(ByteBuffer source, int targetSubpartition) {
        BufferBuilder builder = builders[targetSubpartition];
        int availableBytes = builder != null ? builder.getWritableBytes() : 0;
        if (source.remaining()
                > availableBytes
                        + (numGuaranteedBuffers - numBuffersOccupied) * (long) bufferSize) {
            return;
        }

        do {
            if (builder == null) {
                builder = new BufferBuilder(freeSegments.poll(), bufferRecycler);
                buffers[targetSubpartition].add(builder.createBufferConsumer());
                ++numBuffersOccupied;
                builders[targetSubpartition] = builder;
            }

            builder.append(source);
            if (builder.isFull()) {
                builder.finish();
                builder.close();
                builders[targetSubpartition] = null;
                builder = null;
            }
        } while (source.hasRemaining());
    }

    /**
     * 按子分区顺序读取下一个缓冲区。
     *
     * <p>读取策略：
     * <ul>
     *   <li>按 subpartitionReadOrder 指定的顺序遍历子分区</li>
     *   <li>从当前子分区的队列头部取出 BufferConsumer 并构建 Buffer</li>
     *   <li>当前子分区读完后自动切换到下一个子分区</li>
     *   <li>所有子分区读完后返回 null</li>
     * </ul>
     *
     * @param transitBuffer 未使用（HashBasedDataBuffer 的 Buffer 已包含数据）
     */
    @Override
    public BufferWithSubpartition getNextBuffer(MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        BufferWithSubpartition buffer = null;
        if (!hasRemaining() || readOrderIndex >= subpartitionReadOrder.length) {
            return null;
        }

        int targetSubpartition = subpartitionReadOrder[readOrderIndex];
        while (buffer == null) {
            BufferConsumer consumer = buffers[targetSubpartition].poll();
            if (consumer != null) {
                buffer = new BufferWithSubpartition(consumer.build(), targetSubpartition);
                numBuffersOccupied -= buffer.getBuffer().isBuffer() ? 1 : 0;
                numTotalBytesRead += buffer.getBuffer().readableBytes();
                consumer.close();
            } else {
                if (++readOrderIndex >= subpartitionReadOrder.length) {
                    break;
                }
                targetSubpartition = subpartitionReadOrder[readOrderIndex];
            }
        }
        return buffer;
    }

    @Override
    public long numTotalRecords() {
        return numTotalRecords;
    }

    @Override
    public long numTotalBytes() {
        return numTotalBytes;
    }

    @Override
    public boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    /**
     * 完成写入阶段，进入可读取状态。
     *
     * <p>会将所有子分区中未完成的 BufferBuilder 全部 finish，确保所有数据可被读取。
     */
    @Override
    public void finish() {
        checkState(!isFinished, "DataBuffer is already finished.");

        isFinished = true;
        // 遍历所有子分区，将正在写入的 Builder 全部完成
        for (int subpartition = 0; subpartition < builders.length; ++subpartition) {
            BufferBuilder builder = builders[subpartition];
            if (builder != null) {
                builder.finish();
                builder.close();
                builders[subpartition] = null;
            }
        }
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    /**
     * 释放所有资源。
     *
     * <p>关闭所有 BufferBuilder 和 BufferConsumer，释放其持有的内存段。
     */
    @Override
    public void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;

        for (int subpartition = 0; subpartition < builders.length; ++subpartition) {
            BufferBuilder builder = builders[subpartition];
            if (builder != null) {
                builder.close();
                builders[subpartition] = null;
            }
        }

        for (ArrayDeque<BufferConsumer> buffer : buffers) {
            BufferConsumer consumer = buffer.poll();
            while (consumer != null) {
                consumer.close();
                consumer = buffer.poll();
            }
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }
}
