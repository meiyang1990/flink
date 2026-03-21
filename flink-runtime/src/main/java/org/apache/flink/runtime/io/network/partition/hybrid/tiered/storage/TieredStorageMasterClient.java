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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.AllTieredShuffleMasterSnapshots;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.ShuffleDescriptorRetriever;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.shuffle.DefaultPartitionWithMetrics;
import org.apache.flink.runtime.shuffle.DefaultShuffleMetrics;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;
import org.apache.flink.util.concurrent.FutureUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Client of the Tiered Storage used by the master.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageMasterClient 是 JobManager（Master）侧的分层存储客户端，负责协调所有
 * Tier 层的 Master 端代理（TierMasterAgent）。它是 JobManager 与分层存储系统交互的统一入口。
 *
 * <h2>主要职责</h2>
 *
 * <ul>
 *   <li><b>作业注册/注销</b>: 将作业信息注册到所有 Tier 层</li>
 *   <li><b>分区管理</b>: 添加和释放分区，获取 ShuffleDescriptor</li>
 *   <li><b>状态快照/恢复</b>: 支持 HA 场景下的状态持久化和恢复</li>
 *   <li><b>指标采集</b>: 汇总各 Tier 层的分区指标信息</li>
 * </ul>
 *
 * <h2>与其他组件的关系</h2>
 *
 * <pre>
 *   NettyShuffleMaster
 *           │
 *           │ 使用
 *           ▼
 *   TieredStorageMasterClient
 *           │
 *           │ 管理多个
 *           ▼
 *   ┌──────────────────────────────────────┐
 *   │ TierMasterAgent (Memory/Disk/Remote) │
 *   └──────────────────────────────────────┘
 * </pre>
 *
 * <h2>分区注册流程</h2>
 *
 * <pre>
 *   addPartitionAndGetShuffleDescriptor()
 *           │
 *           │ 遍历所有 Tier
 *           ▼
 *   ┌─────────────────────────────────────────┐
 *   │ TierMasterAgent.addPartitionAndGetXxx() │ ─► TierShuffleDescriptor
 *   └─────────────────────────────────────────┘
 *           │
 *           │ 收集所有 TierShuffleDescriptor
 *           ▼
 *   List<TierShuffleDescriptor> ─► 封装到 NettyShuffleDescriptor
 * </pre>
 */
public class TieredStorageMasterClient {

    // ================================================================================
    //  Tier 层管理
    // ================================================================================

    // Tier 层列表：(Tier标识符, TierMasterAgent) 的有序列表
    // 顺序与 TierShuffleDescriptor 列表的顺序一致
    private final List<Tuple2<String, TierMasterAgent>> tiers;

    // Tier 标识符到 MasterAgent 的映射，用于状态恢复时快速定位
    private final Map<String, TierMasterAgent> tierMasterAgentMap;

    // 是否所有分区数据都存储在远程（用于优化 getPartitionWithMetrics）
    private final boolean allPartitionInRemote;

    // ShuffleDescriptor 检索器，用于获取分区的 Shuffle 描述符
    private final ShuffleDescriptorRetriever shuffleDescriptorRetriever;

    public TieredStorageMasterClient(
            List<Tuple2<String, TierMasterAgent>> tiers,
            ShuffleDescriptorRetriever shuffleDescriptorRetriever) {
        this.tiers = tiers;
        this.allPartitionInRemote = tiers.stream().allMatch(tier -> tier.f1.partitionInRemote());
        this.tierMasterAgentMap = new HashMap<>();
        for (Tuple2<String, TierMasterAgent> tier : tiers) {
            tierMasterAgentMap.put(tier.f0, tier.f1);
        }
        this.shuffleDescriptorRetriever = shuffleDescriptorRetriever;
    }

    /**
     * 将作业注册到所有 Tier 层。
     *
     * @param jobID          作业标识符
     * @param shuffleHandler Tier 层与作业交互的回调处理器
     */
    public void registerJob(JobID jobID, TierShuffleHandler shuffleHandler) {
        tiers.forEach(tierMasterAgent -> tierMasterAgent.f1.registerJob(jobID, shuffleHandler));
    }

    /**
     * 从所有 Tier 层注销作业。
     */
    public void unregisterJob(JobID jobID) {
        tiers.forEach(tierMasterAgent -> tierMasterAgent.f1.unregisterJob(jobID));
    }

    /**
     * 添加分区并获取所有 Tier 层的 ShuffleDescriptor。
     *
     * <p>遍历所有 Tier 层，让每个 TierMasterAgent 注册分区并返回其 TierShuffleDescriptor。
     * 返回的列表顺序与 tiers 列表一致。
     *
     * @param jobID              作业标识符
     * @param numSubpartitions   子分区数量
     * @param resultPartitionID  结果分区标识符
     * @return 各 Tier 层的 ShuffleDescriptor 列表
     */
    public List<TierShuffleDescriptor> addPartitionAndGetShuffleDescriptor(
            JobID jobID, int numSubpartitions, ResultPartitionID resultPartitionID) {
        return tiers.stream()
                .map(
                        tierMasterAgent ->
                                tierMasterAgent.f1.addPartitionAndGetShuffleDescriptor(
                                        jobID, numSubpartitions, resultPartitionID))
                .collect(Collectors.toList());
    }

    /**
     * 释放分区在所有 Tier 层的资源。
     *
     * @param shuffleDescriptor 包含各 Tier 层描述符的 ShuffleDescriptor
     */
    public void releasePartition(ShuffleDescriptor shuffleDescriptor) {
        checkState(shuffleDescriptor instanceof NettyShuffleDescriptor);
        List<TierShuffleDescriptor> tierShuffleDescriptors =
                ((NettyShuffleDescriptor) shuffleDescriptor).getTierShuffleDescriptors();
        if (tierShuffleDescriptors != null && !tierShuffleDescriptors.isEmpty()) {
            checkState(tierShuffleDescriptors.size() == tiers.size());
            // 按 Tier 顺序逐个释放
            for (int i = 0; i < tierShuffleDescriptors.size(); i++) {
                tiers.get(i).f1.releasePartition(tierShuffleDescriptors.get(i));
            }
        }
    }

    /**
     * 创建指定作业的状态快照（用于 HA）。
     */
    public void snapshotState(
            CompletableFuture<AllTieredShuffleMasterSnapshots> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {
        snapshotStateInternal(
                snapshotFuture, (agent, future) -> agent.snapshotState(future, context, jobId));
    }

    /**
     * 创建全局状态快照（用于 HA）。
     */
    public void snapshotState(CompletableFuture<AllTieredShuffleMasterSnapshots> snapshotFuture) {
        snapshotStateInternal(snapshotFuture, TierMasterAgent::snapshotState);
    }

    /**
     * 状态快照的内部实现。
     *
     * <p>并行收集所有 Tier 层的快照，然后合并为 AllTieredShuffleMasterSnapshots。
     */
    private void snapshotStateInternal(
            CompletableFuture<AllTieredShuffleMasterSnapshots> snapshotFuture,
            BiConsumer<TierMasterAgent, CompletableFuture<TieredShuffleMasterSnapshot>>
                    masterAgentConsumer) {
        List<CompletableFuture<Tuple2<String, TieredShuffleMasterSnapshot>>> futures =
                new ArrayList<>(tiers.size());
        // 为每个 Tier 创建快照 Future
        for (Tuple2<String, TierMasterAgent> tier : tiers) {
            CompletableFuture<TieredShuffleMasterSnapshot> future = new CompletableFuture<>();
            futures.add(future.thenApply(snap -> Tuple2.of(tier.f0, snap)));
            masterAgentConsumer.accept(tier.f1, future);
        }

        // 合并所有 Tier 的快照结果
        FutureUtils.combineAll(futures)
                .thenAccept(
                        snapshotWithIdentifiers ->
                                snapshotFuture.complete(
                                        new AllTieredShuffleMasterSnapshots(
                                                snapshotWithIdentifiers)));
    }

    /**
     * 从快照恢复全局状态（用于 HA）。
     */
    public void restoreState(TieredInternalShuffleMasterSnapshot clusterSnapshot) {
        checkState(clusterSnapshot != null);
        AllTieredShuffleMasterSnapshots allTierSnapshots = clusterSnapshot.getAllTierSnapshots();
        Collection<Tuple2<String, TieredShuffleMasterSnapshot>> snapshots =
                allTierSnapshots.getSnapshots();
        // 按 Tier 标识符分发快照进行恢复
        for (Tuple2<String, TieredShuffleMasterSnapshot> identifierWithSnap : snapshots) {
            String identifier = identifierWithSnap.f0;
            tierMasterAgentMap.get(identifier).restoreState(identifierWithSnap.f1);
        }
    }

    /**
     * 从快照列表恢复指定作业的状态（用于 HA）。
     */
    public void restoreState(List<TieredInternalShuffleMasterSnapshot> snapshots, JobID jobId) {
        for (TieredInternalShuffleMasterSnapshot internalSnapshot : snapshots) {
            checkState(internalSnapshot != null);
            AllTieredShuffleMasterSnapshots allTierSnapshots =
                    internalSnapshot.getAllTierSnapshots();
            Collection<Tuple2<String, TieredShuffleMasterSnapshot>> tierSnapshots =
                    allTierSnapshots.getSnapshots();
            for (Tuple2<String, TieredShuffleMasterSnapshot> identifierWithSnap : tierSnapshots) {
                String identifier = identifierWithSnap.f0;
                tierMasterAgentMap.get(identifier).restoreState(identifierWithSnap.f1, jobId);
            }
        }
    }

    /**
     * 获取指定分区集合的指标信息。
     *
     * <p>如果所有分区都在远程存储，则直接从各 Tier 层获取；
     * 否则委托给 JobShuffleContext 从 TaskManager 获取。
     *
     * @param jobShuffleContext  作业 Shuffle 上下文
     * @param timeout            超时时间
     * @param expectedPartitions 期望获取指标的分区集合
     * @return 包含分区指标的 Future
     */
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobShuffleContext jobShuffleContext,
            Duration timeout,
            Set<ResultPartitionID> expectedPartitions) {
        JobID jobId = jobShuffleContext.getJobId();
        // 如果不是所有分区都在远程，使用传统方式从 TM 获取
        if (!allPartitionInRemote) {
            return jobShuffleContext.getPartitionWithMetrics(timeout, expectedPartitions);
        }

        // 从各 Tier 层并行获取指标
        List<CompletableFuture<Map<ResultPartitionID, ShuffleMetrics>>> futures =
                new ArrayList<>(tiers.size());
        for (Tuple2<String, TierMasterAgent> tier : tiers) {
            CompletableFuture<Map<ResultPartitionID, ShuffleMetrics>> tierPartitionMapFuture =
                    tier.f1.getPartitionWithMetrics(jobId, timeout, expectedPartitions);
            futures.add(tierPartitionMapFuture);
        }
        // 合并各 Tier 的指标结果
        return FutureUtils.combineAll(futures)
                .thenApply(
                        allPartitions -> {
                            int tierNums = allPartitions.size();
                            List<PartitionWithMetrics> result = new ArrayList<>();
                            // 遍历期望的分区，收集各 Tier 的字节统计
                            expectedPartitions.forEach(
                                    partitionId -> {
                                        List<ResultPartitionBytes> partitionBytes =
                                                new ArrayList<>();
                                        for (Map<ResultPartitionID, ShuffleMetrics> partitionMap :
                                                allPartitions) {
                                            ShuffleMetrics shuffleMetrics =
                                                    partitionMap.get(partitionId);
                                            if (shuffleMetrics == null) {
                                                break;
                                            }
                                            partitionBytes.add(shuffleMetrics.getPartitionBytes());
                                        }
                                        // 只有所有 Tier 都有数据时才算有效
                                        if (partitionBytes.size() == tierNums) {
                                            Optional<ShuffleDescriptor> shuffleDescriptor =
                                                    shuffleDescriptorRetriever.getShuffleDescriptor(
                                                            jobId, partitionId);
                                            shuffleDescriptor.ifPresent(
                                                    descriptor ->
                                                            result.add(
                                                                    new DefaultPartitionWithMetrics(
                                                                            descriptor,
                                                                            new DefaultShuffleMetrics(
                                                                                    ResultPartitionBytes
                                                                                            .mergeAll(
                                                                                                    partitionBytes)))));
                                        }
                                    });
                            return result;
                        });
    }

    /**
     * 关闭客户端，释放所有 Tier 层的资源。
     */
    public void close() {
        tiers.forEach(tier -> tier.f1.close());
    }
}
