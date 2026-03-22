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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;

import java.io.IOException;
import java.util.Optional;

/**
 * The {@link TierConsumerAgent} is the consumer agent of each tier in tiered store, which could
 * read data from responding tier.
 *
 * <p>【中文说明】Tier 层消费者代理接口。
 *
 * <p>核心职责：
 * <ul>
 *   <li>从特定存储层（Memory/Disk/Remote）读取数据</li>
 *   <li>支持按 Segment 粒度读取 Buffer</li>
 *   <li>处理数据可用性通知，与上层 TieredStorageConsumerClient 协作</li>
 * </ul>
 *
 * <p>读取流程图示：
 * <pre>
 *   TieredStorageConsumerClient
 *           │
 *           ▼
 *   ┌───────────────────────────────────────┐
 *   │     TierConsumerAgent（本接口）         │
 *   │  ┌─────────────────────────────────┐  │
 *   │  │ 1. setup() → 设置内存管理器      │  │
 *   │  │ 2. start() → 启动代理           │  │
 *   │  │ 3. peekNextBufferSubpartitionId │  │  ◄─ 查询下一个有数据的子分区
 *   │  │ 4. getNextBuffer() → Buffer     │  │  ◄─ 获取数据
 *   │  │ 5. close()                      │  │
 *   │  └─────────────────────────────────┘  │
 *   └───────────────────────────────────────┘
 *           │
 *           ▼
 *   具体存储后端（MemoryTierConsumerAgent / DiskTierConsumerAgent / RemoteTierConsumerAgent）
 * </pre>
 *
 * <p>关键设计要点：
 * <ul>
 *   <li>支持按 (partitionId, subpartitionId, segmentId) 三元组定位数据</li>
 *   <li>通过 AvailabilityNotifier 实现异步数据就绪通知</li>
 *   <li>TierShuffleDescriptor 动态更新以支持 Failover 场景</li>
 * </ul>
 */
public interface TierConsumerAgent {

    /**
     * The consumer agent may request buffers from the memory manager. Therefore, the {@link
     * TieredStorageMemoryManager} should be integrated into the tier consumer agent. Since the
     * buffer pool is initialized after the creation of the client, the memory manager need to be
     * assigned after the buffer pool becomes available.
     */
    void setup(TieredStorageMemoryManager memoryManager);

    /** Start the consumer agent. */
    void start();

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available or the subpartition index does not belong to the specified indexSet.
     *
     * @param partitionId The index of the partition which the returned subpartition should belong
     *     to.
     * @param indexSet The indexes of the subpartitions expected.
     */
    int peekNextBufferSubpartitionId(
            TieredStoragePartitionId partitionId, ResultSubpartitionIndexSet indexSet)
            throws IOException;

    /**
     * Get buffer from the consumer agent.
     *
     * @param partitionId the id of partition.
     * @param subpartitionId the id of subpartition.
     * @param segmentId the id of segment.
     * @return buffer.
     */
    Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId)
            throws IOException;

    /**
     * Register the notifier to notify the availability of a subpartition.
     *
     * @param notifier to notify availability.
     */
    void registerAvailabilityNotifier(AvailabilityNotifier notifier);

    /** Update the {@link TierShuffleDescriptor} for the consumer agent. */
    void updateTierShuffleDescriptor(
            TieredStoragePartitionId partitionId,
            TieredStorageInputChannelId inputChannelId,
            TieredStorageSubpartitionId subpartitionId,
            TierShuffleDescriptor tierShuffleDescriptor);

    /** Close the consumer agent. */
    void close() throws IOException;
}
