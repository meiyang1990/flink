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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link HashSubpartitionBufferAccumulator} accumulates the records in a subpartition.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>HashSubpartitionBufferAccumulator 是单个子分区的缓冲区累积器，由 HashBufferAccumulator
 * 创建和管理。每个子分区独立累积数据，当缓冲区写满时自动刷新到下游 Tier 层。
 *
 * <h2>数据写入流程</h2>
 *
 * <pre>
 *   append(record, dataType)
 *           │
 *      ┌────┴────┐
 *      │ 是事件？ │
 *      └────┬────┘
 *       是  │    否
 *           │    │
 *           ▼    ▼
 *   ┌────────────────┐  ┌────────────────────┐
 *   │  writeEvent()  │  │   writeRecord()    │
 *   │ (独占一个缓冲区)│  │ (可跨多个缓冲区)   │
 *   └────────────────┘  └────────────────────┘
 *           │                   │
 *           │    ensureCapacityForRecord()
 *           │           │
 *           │    请求足够的 BufferBuilder
 *           │           │
 *           └─────┬─────┘
 *                 ▼
 *         finishCurrentWritingBuffer()
 *                 │
 *                 ▼
 *         flushFinishedBuffer()
 *                 │
 *                 ▼
 *         传递给 HashBufferAccumulator
 * </pre>
 *
 * <h2>关键特性</h2>
 *
 * <ul>
 *   <li><b>事件独占缓冲区</b>: 事件数据使用堆内存段，确保独立传输</li>
 *   <li><b>记录跨缓冲区</b>: 支持配置是否允许记录拆分到多个缓冲区</li>
 *   <li><b>连续缓冲区计数</b>: 提供后续缓冲区数量信息用于下游流量控制</li>
 *   <li><b>按需申请缓冲区</b>: 只在需要时从内存管理器申请，节省资源</li>
 * </ul>
 *
 * <h2>与 HashBufferAccumulator 的协作</h2>
 *
 * <p>HashBufferAccumulator 为每个子分区创建一个 HashSubpartitionBufferAccumulator，
 * 通过 HashSubpartitionBufferAccumulatorContext 接口提供缓冲区申请和刷新能力。
 */
public class HashSubpartitionBufferAccumulator {

    // ================================================================================
    //  配置参数
    // ================================================================================

    // 所属子分区的标识符
    private final TieredStorageSubpartitionId subpartitionId;

    // 单个缓冲区的大小（字节）
    private final int bufferSize;

    // 上下文接口，提供缓冲区申请和刷新能力（由 HashBufferAccumulator 实现）
    private final HashSubpartitionBufferAccumulatorContext bufferAccumulatorContext;

    // ================================================================================
    //  运行时状态
    // ================================================================================

    // 未完成的 BufferBuilder 队列，队头是当前正在写入的缓冲区
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // 是否允许记录跨缓冲区拆分
    // false: 如果当前缓冲区放不下整条记录，先刷新再写入
    // true:  记录可以拆分到多个缓冲区
    private final boolean isPartialRecordAllowed;

    public HashSubpartitionBufferAccumulator(
            TieredStorageSubpartitionId subpartitionId,
            int bufferSize,
            HashSubpartitionBufferAccumulatorContext bufferAccumulatorContext,
            boolean isPartialRecordAllowed) {
        this.subpartitionId = subpartitionId;
        this.bufferSize = bufferSize;
        this.bufferAccumulatorContext = bufferAccumulatorContext;
        this.isPartialRecordAllowed = isPartialRecordAllowed;
    }

    // ------------------------------------------------------------------------
    //  Called by HashBufferAccumulator
    // ------------------------------------------------------------------------

    /**
     * 追加数据到子分区缓冲区。
     *
     * <p>根据数据类型分别处理：
     * <ul>
     *   <li>事件：调用 writeEvent()，事件独占一个缓冲区</li>
     *   <li>记录：调用 writeRecord()，可能跨多个缓冲区</li>
     * </ul>
     *
     * @param record   待写入的数据
     * @param dataType 数据类型（事件或数据记录）
     */
    public void append(ByteBuffer record, Buffer.DataType dataType) throws IOException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType);
        } else {
            writeRecord(record, dataType);
        }
    }

    /**
     * 关闭子分区累积器，刷新未完成的缓冲区并释放资源。
     */
    public void close() {
        // 先刷新当前正在写入的缓冲区（如果非空）
        finishCurrentWritingBufferIfNotEmpty();
        // 关闭所有剩余的 BufferBuilder
        while (!unfinishedBuffers.isEmpty()) {
            unfinishedBuffers.poll().close();
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /**
     * 写入事件数据。
     *
     * <p>事件的特殊处理：
     * <ol>
     *   <li>先刷新当前缓冲区，确保事件独占</li>
     *   <li>使用堆内存段存储事件（提高网络内存效率）</li>
     *   <li>立即刷新到下游</li>
     * </ol>
     */
    private void writeEvent(ByteBuffer event, Buffer.DataType dataType) {
        checkArgument(dataType.isEvent());

        // Each event should take an exclusive buffer
        // 事件独占缓冲区，先刷新当前写入中的数据
        finishCurrentWritingBufferIfNotEmpty();

        // Store the events in the heap segments to improve network memory efficiency
        // 使用堆内存段包装事件数据，避免占用 Network Buffer
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        flushFinishedBuffer(
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size()), 0);
    }

    /**
     * 写入数据记录。
     */
    private void writeRecord(ByteBuffer record, Buffer.DataType dataType) {
        checkArgument(!dataType.isEvent());

        // 确保有足够的缓冲区容量
        ensureCapacityForRecord(record);

        writeRecord(record);
    }

    /**
     * 确保有足够的缓冲区来存储记录。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>如果不允许部分记录且当前缓冲区放不下，先刷新</li>
     *   <li>计算当前可用字节数</li>
     *   <li>不够则循环申请新缓冲区直到满足需求</li>
     * </ol>
     */
    private void ensureCapacityForRecord(ByteBuffer record) {
        final int numRecordBytes = record.remaining();

        // 不允许部分记录时，如果当前缓冲区放不下整条记录，先刷新
        if (!isPartialRecordAllowed
                && !unfinishedBuffers.isEmpty()
                && unfinishedBuffers.peek().getWritableBytes() < numRecordBytes) {
            finishCurrentWritingBufferIfNotEmpty();
        }

        // 计算当前所有未完成缓冲区的可用空间
        // = 当前缓冲区剩余 + (其他缓冲区数量 * 单个缓冲区大小)
        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        // 空间不足时，持续申请新缓冲区
        while (availableBytes < numRecordBytes) {
            BufferBuilder bufferBuilder = bufferAccumulatorContext.requestBufferBlocking();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    /**
     * 将记录数据写入缓冲区。
     *
     * <p>循环写入直到记录数据全部写完：
     * <ol>
     *   <li>向当前缓冲区追加数据</li>
     *   <li>如果缓冲区写满，完成并刷新</li>
     *   <li>继续写入下一个缓冲区</li>
     * </ol>
     */
    private void writeRecord(ByteBuffer record) {
        boolean needFinalFlush = false;
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer = checkNotNull(unfinishedBuffers.peek());
            // 向缓冲区追加数据
            currentWritingBuffer.append(record);
            // 缓冲区写满时完成并刷新
            if (currentWritingBuffer.isFull()) {
                int numRemainingConsecutiveBuffers = 0;
                if (!isPartialRecordAllowed) {
                    needFinalFlush = true;
                    // 计算剩余数据还需要多少个缓冲区
                    numRemainingConsecutiveBuffers =
                            (int) Math.ceil(((double) record.remaining()) / bufferSize);
                }
                finishCurrentWritingBuffer(numRemainingConsecutiveBuffers);
            }
        }

        // 不允许部分记录时，最后要刷新确保记录边界清晰
        if (needFinalFlush) {
            finishCurrentWritingBuffer(0);
        }
    }

    /**
     * 如果当前缓冲区非空，则完成并刷新。
     */
    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        // 缓冲区为空或完全未使用时跳过
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer(0);
    }

    /**
     * 完成当前缓冲区并刷新到下游。
     *
     * @param numRemainingConsecutiveBuffers 当前记录剩余的后续缓冲区数量
     */
    private void finishCurrentWritingBuffer(int numRemainingConsecutiveBuffers) {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();
        if (currentWritingBuffer == null) {
            return;
        }
        // 不允许部分记录且无后续缓冲区时，标记为 CLEAR_END
        if (currentWritingBuffer.getDataType() == Buffer.DataType.DATA_BUFFER
                && !isPartialRecordAllowed
                && numRemainingConsecutiveBuffers == 0) {
            currentWritingBuffer.setDataType(Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END);
        }
        // 完成写入
        currentWritingBuffer.finish();
        // 转换为可消费的 Buffer
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        // 刷新到下游
        flushFinishedBuffer(buffer, numRemainingConsecutiveBuffers);
    }

    /**
     * 将完成的缓冲区通过上下文接口刷新到下游。
     */
    private void flushFinishedBuffer(Buffer finishedBuffer, int numRemainingConsecutiveBuffers) {
        bufferAccumulatorContext.flushAccumulatedBuffers(
                subpartitionId, finishedBuffer, numRemainingConsecutiveBuffers);
    }
}
