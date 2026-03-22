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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.concurrent.CompletableFuture;

/**
 * 【中文说明】TieredStorageNettyService 是分层存储的 Netty 网络服务接口。
 *
 * <p>核心职责：
 * <ul>
 *   <li>作为 Netty Shuffle 服务的中枢，负责连接 Producer 端代理和 Consumer 端代理。</li>
 *   <li>为 Producer 端提供注册机制，以便后续建立 Netty 连接 Writer。</li>
 *   <li>为 Consumer 端提供注册机制，异步获取 Netty 连接 Reader。</li>
 * </ul>
 */
public interface TieredStorageNettyService {

    /**
     * 【中文说明】注册 Producer 服务。
     *
     * <p>Producer 端代理（如 MemoryTierProducerAgent）通过此回调注册 Netty 服务，
     * 以便后续创建 NettyConnectionWriter 向 Consumer 推送数据。
     *
     * @param partitionId 分区唯一标识
     * @param serviceProducer Netty 服务生成器回调
     */
    void registerProducer(
            TieredStoragePartitionId partitionId, NettyServiceProducer serviceProducer);

    /**
     * 【中文说明】注册 Consumer 服务，获取异步读取连接。
     *
     * <p>Consumer 端代理（如 MemoryTierConsumerAgent）通过此方法注册，
     * 获取一个 NettyConnectionReader 的 Future，该 Reader 用于从远端读取 Shuffle 数据。
     *
     * @param partitionId 分区唯一标识
     * @param subpartitionId 子分区唯一标识
     * @return 异步的 Netty 连接读取器
     */
    CompletableFuture<NettyConnectionReader> registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId);
}
