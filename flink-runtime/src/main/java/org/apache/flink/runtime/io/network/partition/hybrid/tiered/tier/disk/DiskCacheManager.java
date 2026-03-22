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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.concurrent.FutureUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 【中文说明】DiskCacheManager 负责在数据刷写到磁盘文件之前管理缓存的缓冲区。
 *
 * <p>核心职责：
 * <ul>
 *   <li>管理每个子分区的缓冲区缓存</li>
 *   <li>当缓存字节数达到阈值时触发刷写到磁盘</li>
 *   <li>响应内存回收请求，主动刷写缓存</li>
 *   <li>协调 Segment 的生命周期</li>
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 *   ┌─────────────────┐    append     ┌──────────────────┐    flush     ┌──────────────────┐
 *   │  DiskTierProducer│ ───────────► │  DiskCacheManager │ ───────────► │ PartitionFileWriter│
 *   │  Agent          │   缓冲区      │  (本类)           │   批量写入    │                  │
 *   └─────────────────┘              └──────────────────┘              └──────────────────┘
 *                                            │
 *                                            ▼
 *                                    ┌──────────────────┐
 *                                    │ Subpartition     │ × N
 *                                    │ DiskCacheManager │
 *                                    └──────────────────┘
 * </pre>
 *
 * <p>刷写触发条件：
 * <ul>
 *   <li>缓存字节数超过 maxCachedBytesBeforeFlush 阈值</li>
 *   <li>收到内存管理器的回收请求</li>
 *   <li>关闭时强制刷写</li>
 * </ul>
 *
 * <p>线程安全：flushBuffers 方法使用 synchronized 保护，支持多线程安全调用。
 */
class DiskCacheManager {

    // ==================== 分区配置 ====================

    /** 分区 ID */
    private final TieredStoragePartitionId partitionId;

    /** 子分区数量 */
    private final int numSubpartitions;

    /** 触发刷写的缓存字节数阈值 */
    private final int maxCachedBytesBeforeFlush;

    // ==================== 核心组件 ====================

    /** 分区文件写入器，负责将缓冲区写入磁盘 */
    private final PartitionFileWriter partitionFileWriter;

    /** 每个子分区对应的缓存管理器数组 */
    private final SubpartitionDiskCacheManager[] subpartitionCacheManagers;

    // ==================== 运行时状态 ====================

    /** 当前刷写操作是否已完成的 Future */
    private CompletableFuture<Void> hasFlushCompleted;

    /**
     * 所有子分区的缓存字节数计数器。
     * 注意：此计数器仅由 Task 线程访问，无需加锁。
     */
    private int numCachedBytesCounter;

    /**
     * 构造函数。
     *
     * @param partitionId 分区 ID
     * @param numSubpartitions 子分区数量
     * @param maxCachedBytesBeforeFlush 触发刷写的阈值
     * @param memoryManager 内存管理器（用于监听回收请求）
     * @param partitionFileWriter 文件写入器
     */
    DiskCacheManager(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int maxCachedBytesBeforeFlush,
            TieredStorageMemoryManager memoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.partitionId = partitionId;
        this.numSubpartitions = numSubpartitions;
        this.maxCachedBytesBeforeFlush = maxCachedBytesBeforeFlush;
        this.partitionFileWriter = partitionFileWriter;
        this.subpartitionCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        this.hasFlushCompleted = FutureUtils.completedVoidFuture();

        // 为每个子分区创建缓存管理器
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheManagers[subpartitionId] = new SubpartitionDiskCacheManager();
        }
        // 注册内存回收监听器
        memoryManager.listenBufferReclaimRequest(this::notifyFlushCachedBuffers);
    }

    // ------------------------------------------------------------------------
    //  由 DiskTierProducerAgent 调用的方法
    // ------------------------------------------------------------------------

    /** 开始一个新的 Segment */
    void startSegment(int subpartitionId, int segmentIndex) {
        subpartitionCacheManagers[subpartitionId].startSegment(segmentIndex);
    }

    /**
     * 将缓冲区追加到缓存管理器。
     *
     * @param buffer 要缓存的缓冲区
     * @param subpartitionId 目标子分区 ID
     * @param flush 是否允许在追加后检查并触发刷写
     */
    void append(Buffer buffer, int subpartitionId, boolean flush) {
        subpartitionCacheManagers[subpartitionId].append(buffer);
        increaseNumCachedBytesAndCheckFlush(buffer.readableBytes(), flush);
    }

    /**
     * 追加 Segment 结束事件，表示当前 Segment 已完成。
     *
     * @param record Segment 结束事件数据
     * @param subpartitionId 目标子分区 ID
     */
    void appendEndOfSegmentEvent(ByteBuffer record, int subpartitionId) {
        subpartitionCacheManagers[subpartitionId].appendEndOfSegmentEvent(record);
        // Segment 结束时强制检查刷写
        increaseNumCachedBytesAndCheckFlush(record.remaining(), true);
    }

    /**
     * 获取指定子分区当前的缓冲区索引。
     *
     * @param subpartitionId 目标子分区 ID
     * @return 当前缓冲区索引
     */
    int getBufferIndex(int subpartitionId) {
        return subpartitionCacheManagers[subpartitionId].getBufferIndex();
    }

    /** 关闭缓存管理器，强制刷写所有缓存数据，之后不再接受新数据 */
    void close() {
        forceFlushCachedBuffers();
    }

    /** 释放缓存管理器，回收所有内存资源 */
    void release() {
        Arrays.stream(subpartitionCacheManagers).forEach(SubpartitionDiskCacheManager::release);
        partitionFileWriter.release();
    }

    // ------------------------------------------------------------------------
    //  内部方法
    // ------------------------------------------------------------------------

    /**
     * 增加缓存字节计数并检查是否需要刷写。
     * 当缓存字节数超过阈值时触发强制刷写。
     */
    private void increaseNumCachedBytesAndCheckFlush(int numIncreasedCachedBytes, boolean flush) {
        numCachedBytesCounter += numIncreasedCachedBytes;
        if (flush && numCachedBytesCounter > maxCachedBytesBeforeFlush) {
            forceFlushCachedBuffers();
        }
    }

    /** 响应内存回收请求，尝试刷写缓存 */
    private void notifyFlushCachedBuffers() {
        flushBuffers(false);
    }

    /** 强制刷写所有缓存缓冲区 */
    private void forceFlushCachedBuffers() {
        flushBuffers(true);
    }

    /**
     * 执行缓冲区刷写操作。
     *
     * <p>注意：刷写请求可能来自磁盘检查线程或 Task 线程，
     * 因此此方法使用 synchronized 确保线程安全。
     *
     * @param forceFlush 是否强制刷写（忽略上次刷写是否完成）
     */
    private synchronized void flushBuffers(boolean forceFlush) {
        // 非强制模式下，若上次刷写未完成则跳过
        if (!forceFlush && !hasFlushCompleted.isDone()) {
            return;
        }
        // 收集所有子分区待刷写的缓冲区
        List<PartitionFileWriter.SubpartitionBufferContext> buffersToFlush = new ArrayList<>();
        int numToWriteBuffers = getSubpartitionToFlushBuffers(buffersToFlush);

        if (numToWriteBuffers > 0) {
            // 异步写入文件
            CompletableFuture<Void> flushCompletableFuture =
                    partitionFileWriter.write(partitionId, buffersToFlush);
            if (!forceFlush) {
                hasFlushCompleted = flushCompletableFuture;
            }
        }
        // 重置缓存计数器
        numCachedBytesCounter = 0;
    }

    /**
     * 收集所有子分区待刷写的缓冲区。
     *
     * @param buffersToFlush 输出参数，用于存放待刷写的缓冲区上下文
     * @return 待写入的缓冲区总数
     */
    private int getSubpartitionToFlushBuffers(
            List<PartitionFileWriter.SubpartitionBufferContext> buffersToFlush) {
        int numToWriteBuffers = 0;
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            // 从子分区缓存管理器移除所有缓冲区
            List<Tuple2<Buffer, Integer>> bufferWithIndexes =
                    subpartitionCacheManagers[subpartitionId].removeAllBuffers();
            // 构建写入上下文
            buffersToFlush.add(
                    new PartitionFileWriter.SubpartitionBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentBufferContext(
                                            subpartitionCacheManagers[subpartitionId]
                                                    .getSegmentId(),
                                            bufferWithIndexes,
                                            false))));
            numToWriteBuffers += bufferWithIndexes.size();
        }
        return numToWriteBuffers;
    }
}
