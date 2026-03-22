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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 【中文说明】MemoryTierConsumerAgent 是内存层的消费者代理实现。
 *
 * <p>核心职责：
 * <ul>
 *   <li>通过 Netty 连接从内存层读取 Shuffle 数据</li>
 *   <li>管理每个分区/子分区对应的 NettyConnectionReader</li>
 *   <li>提供数据可用性查询和缓冲区读取功能</li>
 * </ul>
 *
 * <p>数据读取流程：
 * <pre>
 *   ┌─────────────────┐     Netty 连接      ┌──────────────────────┐
 *   │  Consumer 端    │ ◄────────────────── │  Producer 端内存层    │
 *   │  (本类)         │   NettyConnection   │  (MemoryTierProducer) │
 *   └─────────────────┘      Reader         └──────────────────────┘
 * </pre>
 *
 * <p>关键特性：
 * <ul>
 *   <li>使用 CompletableFuture 管理异步建立的 Netty 连接</li>
 *   <li>setup/start/close 等生命周期方法为空实现（内存层无需额外初始化）</li>
 *   <li>数据直接通过 Netty 传输，无磁盘 IO</li>
 * </ul>
 */
public class MemoryTierConsumerAgent implements TierConsumerAgent {

    // 存储每个分区每个子分区对应的 Netty 连接读取器（异步获取）
    // 结构：partitionId -> (subpartitionId -> Future<NettyConnectionReader>)
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, CompletableFuture<NettyConnectionReader>>>
            nettyConnectionReaders = new HashMap<>();

    /**
     * 构造函数：为每个消费规格中的分区和子分区注册 Netty 消费者连接。
     *
     * @param tieredStorageConsumerSpecs 消费者规格列表，描述要消费的分区和子分区
     * @param nettyService Netty 服务，用于注册消费者并获取连接
     */
    public MemoryTierConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        // 遍历所有消费规格，为每个子分区注册 Netty 消费者
        for (TieredStorageConsumerSpec tieredStorageConsumerSpec : tieredStorageConsumerSpecs) {
            TieredStoragePartitionId partitionId = tieredStorageConsumerSpec.getPartitionId();
            for (int subpartitionId : tieredStorageConsumerSpec.getSubpartitionIds().values()) {
                // 注册消费者，获取异步的 NettyConnectionReader
                nettyConnectionReaders
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .put(
                                new TieredStorageSubpartitionId(subpartitionId),
                                nettyService.registerConsumer(
                                        partitionId,
                                        new TieredStorageSubpartitionId(subpartitionId)));
            }
        }
    }

    @Override
    public void setup(TieredStorageMemoryManager memoryManager) {
        // noop
    }

    @Override
    public void start() {
        // noop
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        // noop
    }

    /**
     * 查看下一个可读缓冲区所属的子分区 ID。
     *
     * <p>遍历该分区下所有子分区的 NettyConnectionReader，找到第一个有数据可读且在 indexSet 中的子分区。
     *
     * @param partitionId 分区 ID
     * @param indexSet 允许读取的子分区索引集合
     * @return 有数据可读的子分区 ID，若无则返回 -1
     */
    @Override
    public int peekNextBufferSubpartitionId(
            TieredStoragePartitionId partitionId, ResultSubpartitionIndexSet indexSet)
            throws IOException {
        for (CompletableFuture<NettyConnectionReader> readerFuture :
                nettyConnectionReaders.get(partitionId).values()) {
            int subpartitionId;
            try {
                // 阻塞等待 NettyConnectionReader 就绪，然后 peek 下一个可读的子分区 ID
                subpartitionId = readerFuture.get().peekNextBufferSubpartitionId();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to peek subpartition Id.", e);
            }
            // 检查该子分区是否在允许读取的范围内
            if (indexSet.contains(subpartitionId)) {
                return subpartitionId;
            }
        }
        return -1;
    }

    /**
     * 从指定分区/子分区/Segment 读取下一个缓冲区。
     *
     * @param partitionId 分区 ID
     * @param subpartitionId 子分区 ID
     * @param segmentId Segment ID（用于定位数据位置）
     * @return 读取到的缓冲区（可能为空）
     */
    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        try {
            // 通过 NettyConnectionReader 读取缓冲区数据
            return nettyConnectionReaders
                    .get(partitionId)
                    .get(subpartitionId)
                    .get()
                    .readBuffer(subpartitionId.getSubpartitionId(), segmentId);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get next buffer.", e);
        }
    }

    @Override
    public void updateTierShuffleDescriptor(
            TieredStoragePartitionId partitionId,
            TieredStorageInputChannelId inputChannelId,
            TieredStorageSubpartitionId subpartitionId,
            TierShuffleDescriptor tierShuffleDescriptor) {
        // noop
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
