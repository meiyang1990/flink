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

import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

/**
 * Describe the different data sources in {@link TieredStorageConsumerClient}.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageConsumerSpec 描述消费者端的数据源规格，用于在 TieredStorageConsumerClient
 * 中标识和定位需要读取的数据。每个 ConsumerSpec 对应一个上游分区的特定子分区集合。
 *
 * <h2>关键属性</h2>
 *
 * <ul>
 *   <li><b>gateIndex</b>: InputGate 索引，标识消费者端的输入门</li>
 *   <li><b>partitionId</b>: 上游分区标识符</li>
 *   <li><b>inputChannelId</b>: 输入通道标识符，用于数据可用性通知</li>
 *   <li><b>subpartitionIds</b>: 需要消费的子分区索引集合</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <pre>
 *   消费者启动流程:
 *   ───────────────
 *   TieredStorageConsumerClient.setup()
 *           │
 *           │ 遍历所有 ConsumerSpec
 *           ▼
 *   ┌─────────────────────────────────────┐
 *   │ 为每个 spec 创建 TierConsumerAgent │
 *   │ 建立 partitionId -> inputChannelId │
 *   │ 映射关系                            │
 *   └─────────────────────────────────────┘
 * </pre>
 */
public class TieredStorageConsumerSpec {

    // InputGate 索引，用于标识消费者端的输入门
    private final int gateIndex;

    // 上游分区的标识符
    private final TieredStoragePartitionId tieredStoragePartitionId;

    // 输入通道标识符，用于数据可用性通知回调
    private final TieredStorageInputChannelId tieredStorageInputChannelId;

    // 需要消费的子分区索引集合（支持范围或离散集合）
    private final ResultSubpartitionIndexSet tieredStorageSubpartitionIds;

    public TieredStorageConsumerSpec(
            int gateIndex,
            TieredStoragePartitionId tieredStoragePartitionId,
            TieredStorageInputChannelId tieredStorageInputChannelId,
            ResultSubpartitionIndexSet tieredStorageSubpartitionIds) {
        this.gateIndex = gateIndex;
        this.tieredStoragePartitionId = tieredStoragePartitionId;
        this.tieredStorageInputChannelId = tieredStorageInputChannelId;
        this.tieredStorageSubpartitionIds = tieredStorageSubpartitionIds;
    }

    public int getGateIndex() {
        return gateIndex;
    }

    public TieredStoragePartitionId getPartitionId() {
        return tieredStoragePartitionId;
    }

    public TieredStorageInputChannelId getInputChannelId() {
        return tieredStorageInputChannelId;
    }

    public ResultSubpartitionIndexSet getSubpartitionIds() {
        return tieredStorageSubpartitionIds;
    }
}
