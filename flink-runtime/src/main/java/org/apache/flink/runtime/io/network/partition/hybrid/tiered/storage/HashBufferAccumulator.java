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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The hash implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator} receives
 * the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>To avoid the buffer waiting deadlock between the subpartitions, the {@link
 * HashBufferAccumulator} requires at least n buffers (n is the number of subpartitions) to make
 * sure that each subpartition has at least one buffer to accumulate the receiving data. Once an
 * accumulated buffer is finished, the buffer will be flushed immediately.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>基于哈希的缓冲区累积器实现，为每个子分区维护独立的缓冲区。
 *
 * <h3>内部结构</h3>
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────┐
 * │                    HashBufferAccumulator                         │
 * │                                                                  │
 * │  ┌────────────────────────────────────────────────────────────┐  │
 * │  │         HashSubpartitionBufferAccumulator[]                │  │
 * │  │  ┌─────────────┐ ┌─────────────┐     ┌─────────────┐       │  │
 * │  │  │ SP 0 累积器 │ │ SP 1 累积器 │ ... │ SP n 累积器 │       │  │
 * │  │  │             │ │             │     │             │       │  │
 * │  │  │ ┌─────────┐ │ │ ┌─────────┐ │     │ ┌─────────┐ │       │  │
 * │  │  │ │ Buffer  │ │ │ │ Buffer  │ │     │ │ Buffer  │ │       │  │
 * │  │  │ └─────────┘ │ │ └─────────┘ │     │ └─────────┘ │       │  │
 * │  │  └─────────────┘ └─────────────┘     └─────────────┘       │  │
 * │  └────────────────────────────────────────────────────────────┘  │
 * │                                                                  │
 * │  receive(record, subpartitionId) → 路由到对应子分区累积器        │
 * │                                                                  │
 * │  缓冲区写满 → flushAccumulatedBuffers() → bufferFlusher.accept() │
 * │                                                                  │
 * └──────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>死锁预防</h3>
 * <p>为避免子分区间的缓冲区等待死锁，HashBufferAccumulator 要求至少有 n 个缓冲区
 * （n = 子分区数），确保每个子分区至少有一个缓冲区用于累积数据。
 *
 * <h3>线程安全</h3>
 * <p>此类无需线程安全，因为只会从主线程访问。
 */
public class HashBufferAccumulator
        implements BufferAccumulator, HashSubpartitionBufferAccumulatorContext {

    // 内存管理器，用于请求和回收缓冲区
    private final TieredStorageMemoryManager memoryManager;

    // 各子分区的缓冲区累积器数组
    private final HashSubpartitionBufferAccumulator[] hashSubpartitionBufferAccumulators;

    /**
     * The {@link HashBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     */
    // 累积缓冲区刷出回调，在 setup 时初始化
    @Nullable
    private TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> accumulatedBufferFlusher;

    /**
     * 构造函数，为每个子分区创建独立的累积器。
     *
     * @param numSubpartitions 子分区数量
     * @param bufferSize 单个缓冲区大小
     * @param memoryManager 内存管理器
     * @param isPartialRecordAllowed 是否允许部分记录（跨缓冲区）
     */
    public HashBufferAccumulator(
            int numSubpartitions,
            int bufferSize,
            TieredStorageMemoryManager memoryManager,
            boolean isPartialRecordAllowed) {
        this.memoryManager = memoryManager;
        this.hashSubpartitionBufferAccumulators =
                new HashSubpartitionBufferAccumulator[numSubpartitions];
        // 为每个子分区创建累积器
        for (int i = 0; i < numSubpartitions; i++) {
            hashSubpartitionBufferAccumulators[i] =
                    new HashSubpartitionBufferAccumulator(
                            new TieredStorageSubpartitionId(i),
                            bufferSize,
                            this,
                            isPartialRecordAllowed);
        }
    }

    /** 初始化累积器，注册缓冲区刷出回调 */
    @Override
    public void setup(
            TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> accumulatedBufferFlusher) {
        this.accumulatedBufferFlusher = accumulatedBufferFlusher;
    }

    /** 接收记录，路由到对应子分区的累积器 */
    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        getSubpartitionAccumulator(subpartitionId).append(record, dataType);
    }

    /** 关闭所有子分区累积器 */
    @Override
    public void close() {
        Arrays.stream(hashSubpartitionBufferAccumulators)
                .forEach(HashSubpartitionBufferAccumulator::close);
    }

    // -------------------------------------------------------------------------
    //  HashSubpartitionBufferAccumulatorContext 接口实现
    // -------------------------------------------------------------------------

    /** 以阻塞方式请求缓冲区（供子分区累积器调用） */
    @Override
    public BufferBuilder requestBufferBlocking() {
        return memoryManager.requestBufferBlocking(this);
    }

    /** 刷出累积的缓冲区（供子分区累积器调用） */
    @Override
    public void flushAccumulatedBuffers(
            TieredStorageSubpartitionId subpartitionId,
            Buffer accumulatedBuffer,
            int numRemainingConsecutiveBuffers) {
        checkNotNull(accumulatedBufferFlusher)
                .accept(subpartitionId, accumulatedBuffer, numRemainingConsecutiveBuffers);
    }

    /** 根据子分区 ID 获取对应的累积器 */
    private HashSubpartitionBufferAccumulator getSubpartitionAccumulator(
            TieredStorageSubpartitionId subpartitionId) {
        return hashSubpartitionBufferAccumulators[subpartitionId.getSubpartitionId()];
    }
}
