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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/** A factory that creates all the components of a tier. */
public interface TierFactory {

    /** Sets up the tier factory based on the {@link Configuration}. */
    void setup(Configuration configuration);

    /** Get the {@link TieredStorageMemorySpec} of the master-side agent. */
    TieredStorageMemorySpec getMasterAgentMemorySpec();

    /** Get the {@link TieredStorageMemorySpec} of the producer-side agent. */
    TieredStorageMemorySpec getProducerAgentMemorySpec();

    /** Get the {@link TieredStorageMemorySpec} of the consumer-side agent. */
    TieredStorageMemorySpec getConsumerAgentMemorySpec();

    /** Creates the master-side agent of a Tier. */
    TierMasterAgent createMasterAgent(TieredStorageResourceRegistry tieredStorageResourceRegistry);

    /**
     * Creates the producer-side agent of a Tier.
     *
     * <p>【中文说明】创建生产者端代理。用于在 TaskManager 上写入数据。
     *
     * <p>参数说明：
     * <ul>
     *   <li>numPartitions/numSubpartitions - 分区配置</li>
     *   <li>dataFileBasePath - 数据文件基础路径（磁盘层使用）</li>
     *   <li>isBroadcastOnly - 是否为广播模式</li>
     *   <li>storageMemoryManager - 内存管理器</li>
     *   <li>nettyService - Netty 网络服务（内存层使用）</li>
     *   <li>bufferPool - 批处理 Shuffle 读取缓冲池（磁盘层使用）</li>
     *   <li>bufferCompressor - 可选的压缩器</li>
     * </ul>
     *
     * @return Producer 端代理实例
     */
    TierProducerAgent createProducerAgent(
            int numPartitions,
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            List<TierShuffleDescriptor> shuffleDescriptors,
            int maxRequestedBuffer,
            @Nullable BufferCompressor bufferCompressor);

    /**
     * Creates the consumer-side agent of a Tier.
     *
     * <p>【中文说明】创建消费者端代理。用于在 TaskManager 上读取数据。
     *
     * @param tieredStorageConsumerSpecs 消费者规格列表（包含分区和子分区信息）
     * @param shuffleDescriptors Shuffle 描述符列表（包含数据定位信息）
     * @param nettyService Netty 网络服务
     * @return Consumer 端代理实例
     */
    TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors,
            TieredStorageNettyService nettyService);

    /**
     * The unique identifier of this tier.
     *
     * <p>【中文说明】获取此 Tier 的唯一标识符。
     *
     * <p>内置标识符：
     * <ul>
     *   <li>"memory" - 内存层</li>
     *   <li>"disk" - 磁盘层</li>
     *   <li>"remote" - 远程层</li>
     * </ul>
     *
     * @return Tier 唯一标识字符串
     */
    String identifier();
}
