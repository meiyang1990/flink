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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.EmptyTieredShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredShuffleMasterSnapshot;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The master-side agent of a Tier.
 *
 * <p>【中文说明】Tier 层 Master 端代理接口。
 *
 * <p>核心职责：
 * <ul>
 *   <li>管理作业级别的分区注册与注销</li>
 *   <li>生成 TierShuffleDescriptor（包含数据定位信息）供消费者使用</li>
 *   <li>支持状态快照与恢复（用于 Failover）</li>
 *   <li>处理分区指标查询（如子分区大小）</li>
 * </ul>
 *
 * <p>组件关系图示：
 * <pre>
 *   JobMaster / ShuffleMaster
 *           │
 *           ▼
 *   ┌───────────────────────────────────────┐
 *   │      TierMasterAgent（本接口）          │
 *   │  ┌─────────────────────────────────┐  │
 *   │  │ 1. registerJob()                │  │  ◄─ 作业注册
 *   │  │ 2. addPartitionAndGetDescriptor │  │  ◄─ 分区注册，返回 Descriptor
 *   │  │ 3. snapshotState/restoreState   │  │  ◄─ 状态管理
 *   │  │ 4. releasePartition()           │  │  ◄─ 分区释放
 *   │  │ 5. unregisterJob()              │  │  ◄─ 作业注销
 *   │  └─────────────────────────────────┘  │
 *   └───────────────────────────────────────┘
 *           │
 *           ▼
 *   具体存储后端（MemoryTier 无 Master / DiskTier 无 Master / RemoteTierMasterAgent）
 * </pre>
 *
 * <p>关键设计要点：
 * <ul>
 *   <li>TierShuffleDescriptor 是数据定位的核心，包含消费者连接生产者所需的所有信息</li>
 *   <li>partitionInRemote() 标识分区是否存储在远程集群（而非 TaskManager 本地）</li>
 *   <li>状态快照用于 HA 场景下的 Failover 恢复</li>
 * </ul>
 */
public interface TierMasterAgent {

    /**
     * Register a job id with a {@link TierShuffleHandler}.
     *
     * <p>【中文说明】注册作业。将 JobID 与 TierShuffleHandler 关联，用于后续的分区管理操作。
     *
     * @param jobID 作业唯一标识
     * @param tierShuffleHandler 用于处理该 Tier 层 Shuffle 操作的处理器
     */
    void registerJob(JobID jobID, TierShuffleHandler tierShuffleHandler);

    /**
     * Unregister a job id.
     *
     * <p>【中文说明】注销作业。作业完成或取消时调用，清理相关资源。
     *
     * @param jobID 作业唯一标识
     */
    void unregisterJob(JobID jobID);

    /**
     * Add a new tiered storage partition and get the {@link TierShuffleDescriptor}.
     *
     * <p>【中文说明】添加新分区并获取 Shuffle 描述符。
     *
     * <p>这是分区注册的核心方法。返回的 TierShuffleDescriptor 包含消费者定位数据所需的信息。
     *
     * @param jobID 所属作业 ID
     * @param numSubpartitions 子分区数量
     * @param resultPartitionID 结果分区 ID
     * @return TierShuffleDescriptor 包含数据定位信息的描述符
     */
    TierShuffleDescriptor addPartitionAndGetShuffleDescriptor(
            JobID jobID, int numSubpartitions, ResultPartitionID resultPartitionID);

    /** Triggers a snapshot of the tier master agent's state which related the specified job. */
    default void snapshotState(
            CompletableFuture<TieredShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {
        snapshotFuture.complete(EmptyTieredShuffleMasterSnapshot.getInstance());
    }

    /** Triggers a snapshot of the tier master agent's state. */
    default void snapshotState(CompletableFuture<TieredShuffleMasterSnapshot> snapshotFuture) {
        snapshotFuture.complete(EmptyTieredShuffleMasterSnapshot.getInstance());
    }

    /** Restores the state of the tier master agent from the provided snapshots. */
    default void restoreState(TieredShuffleMasterSnapshot snapshot, JobID jobId) {}

    /**
     * Restores the state of the tier master agent from the provided snapshots for the specified
     * job.
     */
    default void restoreState(TieredShuffleMasterSnapshot snapshot) {}

    /**
     * Retrieves specified partitions and their metrics (identified by {@code expectedPartitions}),
     * the metrics include sizes of sub-partitions in a result partition.
     *
     * @param jobId ID of the target job
     * @param timeout The timeout used for retrieve the specified partitions.
     * @param expectedPartitions The set of identifiers for the result partitions whose metrics are
     *     to be fetched.
     * @return A future will contain a map of the partitions with their metrics that could be
     *     retrieved from the expected partitions within the specified timeout period.
     */
    default CompletableFuture<Map<ResultPartitionID, ShuffleMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        if (!partitionInRemote()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            throw new UnsupportedOperationException(
                    "remote partition should be reported by tier itself.");
        }
    }

    /**
     * Release a tiered storage partition.
     *
     * @param shuffleDescriptor the partition shuffle descriptor to be released
     */
    void releasePartition(TierShuffleDescriptor shuffleDescriptor);

    /** Close this tier master agent. */
    void close();

    /** Is this tier manage the partition in remote cluster instead of flink taskmanager. */
    default boolean partitionInRemote() {
        return false;
    }
}
