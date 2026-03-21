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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TieredStorageConsumerClient} is used to read buffer from tiered store.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>Tiered Storage 消费者客户端，负责从分层存储读取数据。
 *
 * <h3>读取流程</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │               TieredStorageConsumerClient                       │
 * │                                                                 │
 * │  getNextBuffer(partitionId, subpartitionId)                     │
 * │      │                                                          │
 * │      ▼                                                          │
 * │  ┌─────────────────────────────────────────────────────────────┐│
 * │  │ currentConsumerAgentAndSegmentIds 中是否有当前代理？         ││
 * │  │   有：直接从该代理读取                                       ││
 * │  │   无：按优先级遍历所有代理尝试读取                           ││
 * │  └─────────────────────────────────────────────────────────────┘│
 * │      │                                                          │
 * │      ▼                                                          │
 * │  TierConsumerAgent.getNextBuffer()                              │
 * │      │                                                          │
 * │      ▼                                                          │
 * │  ┌─────────────────────────────────────────────────────────────┐│
 * │  │ 读取到 END_OF_SEGMENT ?                                      ││
 * │  │   是：递增 segmentId，清空当前代理，递归读取下一个           ││
 * │  │   否：返回缓冲区数据                                         ││
 * │  └─────────────────────────────────────────────────────────────┘│
 * │                                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>Segment 切换机制</h3>
 * <p>每个（partitionId, subpartitionId）组合维护当前正在读取的 Segment 索引和对应的消费者代理。
 * 当读取到 END_OF_SEGMENT 标记时，递增 Segment 索引并清空代理引用，下次读取会重新选择代理。
 *
 * <h3>多层优先级</h3>
 * <p>消费者代理按优先级排序，读取时优先从高优先级层（如 Memory Tier）获取数据。
 */
public class TieredStorageConsumerClient {

    // -------------------------------------------------------------------------
    //  核心组件
    // -------------------------------------------------------------------------

    // 存储层工厂列表，用于创建消费者代理
    private final List<TierFactory> tierFactories;

    // Netty 服务，用于网络通信
    private final TieredStorageNettyService nettyService;

    // 各存储层的消费者代理列表，按优先级排序
    private final List<TierConsumerAgent> tierConsumerAgents;

    // -------------------------------------------------------------------------
    //  读取状态
    // -------------------------------------------------------------------------

    /**
     * This map is used to record the consumer agent being used and the id of segment being read for
     * each data source, which is represented by {@link TieredStoragePartitionId} and {@link
     * TieredStorageSubpartitionId}.
     */
    // 记录每个（partitionId, subpartitionId）当前正在使用的消费者代理和 Segment 索引
    // Tuple2<当前代理, 当前 Segment 索引>，代理为 null 表示需要重新选择
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<TierConsumerAgent, Integer>>>
            currentConsumerAgentAndSegmentIds = new HashMap<>();

    /**
     * 构造函数，创建各存储层的消费者代理。
     *
     * @param tierFactories 存储层工厂列表
     * @param tieredStorageConsumerSpecs 消费者规格列表
     * @param tierShuffleDescriptors 各分区的 Shuffle 描述符（外层按分区，内层按存储层）
     * @param nettyService Netty 服务
     */
    public TieredStorageConsumerClient(
            List<TierFactory> tierFactories,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<List<TierShuffleDescriptor>> tierShuffleDescriptors,
            TieredStorageNettyService nettyService) {
        this.tierFactories = tierFactories;
        this.nettyService = nettyService;
        this.tierConsumerAgents =
                createTierConsumerAgents(tieredStorageConsumerSpecs, tierShuffleDescriptors);
    }

    /**
     * 初始化消费者客户端，设置内存管理器。
     */
    public void setup(BufferPool bufferPool) {
        // 创建一个不支持回收的内存管理器（消费端无需回收机制）
        TieredStorageMemoryManagerImpl memoryManager = new TieredStorageMemoryManagerImpl(0, false);
        memoryManager.setup(bufferPool, Collections.emptyList());
        // 为所有消费者代理设置内存管理器
        tierConsumerAgents.forEach(tierConsumerAgent -> tierConsumerAgent.setup(memoryManager));
    }

    /** 启动所有消费者代理 */
    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available or the subpartition index does not belong to the specified indexSet.
     *
     * <p>窥视下一个有数据的子分区索引，用于 InputGate 的轮询调度。
     *
     * @param partitionId The index of the partition which the returned subpartition should belong
     *     to.
     * @param indexSet The indexes of the subpartitions expected.
     * @return 有数据的子分区索引，无数据返回 -1
     */
    public int peekNextBufferSubpartitionId(
            TieredStoragePartitionId partitionId, ResultSubpartitionIndexSet indexSet)
            throws IOException {
        // 遍历所有消费者代理，返回第一个有数据的子分区索引
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            int subpartitionId =
                    tierConsumerAgent.peekNextBufferSubpartitionId(partitionId, indexSet);
            if (subpartitionId >= 0) {
                return subpartitionId;
            }
        }
        return -1;
    }

    /**
     * 获取下一个缓冲区。
     *
     * <p>读取逻辑：
     * <ol>
     *   <li>若当前有代理，直接从该代理读取
     *   <li>否则按优先级遍历所有代理尝试读取
     *   <li>若读取到 END_OF_SEGMENT，递增 Segment 索引并递归读取下一个
     * </ol>
     */
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId)
            throws IOException {
        // 获取当前（partition, subpartition）的读取状态
        Tuple2<TierConsumerAgent, Integer> currentConsumerAgentAndSegmentId =
                currentConsumerAgentAndSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(null, 0));
        Optional<Buffer> buffer = Optional.empty();
        if (currentConsumerAgentAndSegmentId.f0 == null) {
            // 当前无代理，按优先级遍历所有代理尝试读取
            for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
                buffer =
                        tierConsumerAgent.getNextBuffer(
                                partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
                if (buffer.isPresent()) {
                    // 找到有数据的代理，记录下来
                    currentConsumerAgentAndSegmentIds
                            .get(partitionId)
                            .put(
                                    subpartitionId,
                                    Tuple2.of(
                                            tierConsumerAgent,
                                            currentConsumerAgentAndSegmentId.f1));
                    break;
                }
            }
        } else {
            // 有当前代理，直接从该代理读取
            buffer =
                    currentConsumerAgentAndSegmentId.f0.getNextBuffer(
                            partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
        }
        if (!buffer.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = buffer.get();
        // 检查是否为 Segment 结束标记
        if (bufferData.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
            // 递增 Segment 索引，清空当前代理（下次读取会重新选择）
            currentConsumerAgentAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(null, currentConsumerAgentAndSegmentId.f1 + 1));
            // 回收 END_OF_SEGMENT 标记缓冲区
            bufferData.recycleBuffer();
            // 递归读取下一个 Segment 的数据
            return getNextBuffer(partitionId, subpartitionId);
        }
        return Optional.of(bufferData);
    }

    /** 注册数据可用性通知器，用于异步通知有数据可读 */
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.registerAvailabilityNotifier(notifier);
        }
    }

    /**
     * 更新 Shuffle 描述符，用于运行时动态更新分区位置信息。
     */
    public void updateTierShuffleDescriptors(
            TieredStoragePartitionId partitionId,
            TieredStorageInputChannelId inputChannelId,
            TieredStorageSubpartitionId subpartitionId,
            List<TierShuffleDescriptor> tierShuffleDescriptors) {
        checkState(tierShuffleDescriptors.size() == tierConsumerAgents.size());
        for (int i = 0; i < tierShuffleDescriptors.size(); i++) {
            tierConsumerAgents
                    .get(i)
                    .updateTierShuffleDescriptor(
                            partitionId,
                            inputChannelId,
                            subpartitionId,
                            tierShuffleDescriptors.get(i));
        }
    }

    /** 关闭所有消费者代理 */
    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal methods
    // --------------------------------------------------------------------------------------------

    /**
     * 创建各存储层的消费者代理。
     *
     * @param tieredStorageConsumerSpecs 消费者规格列表
     * @param shuffleDescriptors 原始 Shuffle 描述符（按分区 x 存储层组织）
     * @return 消费者代理列表
     */
    private List<TierConsumerAgent> createTierConsumerAgents(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<List<TierShuffleDescriptor>> shuffleDescriptors) {
        ArrayList<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();

        // 转换 Shuffle 描述符的组织方式
        List<List<TierShuffleDescriptor>> transformedTierShuffleDescriptors =
                transformTierShuffleDescriptors(shuffleDescriptors);
        // Each tier only requires one inner list of transformedTierShuffleDescriptors, so the size
        // of transformedTierShuffleDescriptors and the size of tierFactories are the same.
        checkState(transformedTierShuffleDescriptors.size() == tierFactories.size());
        // 为每个存储层创建消费者代理
        for (int i = 0; i < tierFactories.size(); i++) {
            tierConsumerAgents.add(
                    tierFactories
                            .get(i)
                            .createConsumerAgent(
                                    tieredStorageConsumerSpecs,
                                    transformedTierShuffleDescriptors.get(i),
                                    nettyService));
        }
        return tierConsumerAgents;
    }

    /**
     * Before transforming the shuffle descriptors, the number of tier shuffle descriptors is
     * numPartitions * numTiers (That means shuffleDescriptors.size() is numPartitions, while the
     * shuffleDescriptors.get(0).size() is numTiers). After transforming, the number of tier shuffle
     * descriptors is numTiers * numPartitions (That means transformedList.size() is numTiers, while
     * transformedList.get(0).size() is numPartitions).
     *
     * <p>转换 Shuffle 描述符的组织方式：
     * <pre>
     * 转换前：[分区0:[层0,层1,层2], 分区1:[层0,层1,层2], ...]
     * 转换后：[层0:[分区0,分区1,...], 层1:[分区0,分区1,...], ...]
     * </pre>
     * 转换后每个存储层可以直接获取其所有分区的描述符。
     */
    private static List<List<TierShuffleDescriptor>> transformTierShuffleDescriptors(
            List<List<TierShuffleDescriptor>> shuffleDescriptors) {
        int numTiers = 0;
        int numPartitions = shuffleDescriptors.size();
        // 验证所有分区的存储层数量一致
        for (List<TierShuffleDescriptor> tierShuffleDescriptors : shuffleDescriptors) {
            if (numTiers == 0) {
                numTiers = tierShuffleDescriptors.size();
            }
            checkState(numTiers == tierShuffleDescriptors.size());
        }

        // 按存储层重新组织描述符
        List<List<TierShuffleDescriptor>> transformedList = new ArrayList<>();
        for (int i = 0; i < numTiers; i++) {
            List<TierShuffleDescriptor> innerList = new ArrayList<>();
            for (int j = 0; j < numPartitions; j++) {
                innerList.add(shuffleDescriptors.get(j).get(i));
            }
            transformedList.add(innerList);
        }
        return transformedList;
    }
}
