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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

/**
 * {@link AvailabilityNotifier} is used to notify that the data from the specific partition and
 * subpartition in tiered storage is available.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>AvailabilityNotifier 是分层存储中的数据可用性通知接口，用于在生产者端写入数据后，
 * 通知消费者端相应的分区/子分区有新数据可读。
 *
 * <h2>使用场景</h2>
 *
 * <pre>
 *   Producer 端                         Consumer 端
 *   ============                        ============
 *
 *   TierProducerAgent                   TierConsumerAgent
 *        │                                    ▲
 *        │ 写入数据                           │
 *        ▼                                    │
 *   ┌─────────────────┐                       │
 *   │ Storage Tier    │                       │
 *   │ (Memory/Disk/   │                       │
 *   │  Remote)        │                       │
 *   └─────────────────┘                       │
 *        │                                    │
 *        │ 数据就绪                           │
 *        ▼                                    │
 *   ┌─────────────────────┐      notifyAvailable()
 *   │ AvailabilityNotifier │ ─────────────────┘
 *   └─────────────────────┘
 * </pre>
 *
 * <h2>典型实现</h2>
 *
 * <ul>
 *   <li>TieredStorageConsumerClient 实现此接口，接收到通知后触发数据读取</li>
 *   <li>各 Tier 层的 ConsumerAgent 在数据就绪时调用此接口</li>
 * </ul>
 */
public interface AvailabilityNotifier {

    /**
     * Notify that the data for the specific partition and input channel is available in tiered
     * storage.
     *
     * <p>通知指定分区和输入通道的数据已在分层存储中就绪，可以读取。
     *
     * @param partitionId the partition id. 分区标识符
     * @param inputChannelId the input channel id. 输入通道标识符（对应消费者侧的 InputChannel）
     */
    void notifyAvailable(
            TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId);
}
