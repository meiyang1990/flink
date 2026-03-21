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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.AllTieredShuffleMasterSnapshots;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMaster;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE;
import static org.apache.flink.configuration.ExecutionOptions.BATCH_SHUFFLE_MODE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default {@link ShuffleMaster} for netty and local file based shuffle implementation.
 *
 * <h2>Netty Shuffle 主控实现 - 中文说明</h2>
 *
 * <p>这是 Flink 默认的 Shuffle 主控实现，运行在 JobMaster 端，负责管理基于 Netty 和本地文件的数据交换。
 *
 * <h3>核心职责</h3>
 * <ul>
 *   <li><b>分区注册</b>：接收 Task 生产者的分区注册，生成 ShuffleDescriptor 供消费者定位数据</li>
 *   <li><b>内存计算</b>：根据 Task 的输入输出特征计算所需的网络缓冲区内存</li>
 *   <li><b>分区释放</b>：协调外部分区（如 Tiered Storage）的资源释放</li>
 *   <li><b>作业管理</b>：维护作业级别的 Shuffle 上下文和分区元数据</li>
 *   <li><b>状态快照/恢复</b>：支持 JobMaster Failover 时的 Shuffle 状态持久化与恢复</li>
 * </ul>
 *
 * <h3>支持的 Shuffle 模式</h3>
 * <ul>
 *   <li><b>流式 Shuffle</b>：生产者和消费者同时运行，数据通过 Netty 传输</li>
 *   <li><b>批式 Shuffle</b>：生产者先完成，数据落盘后消费者再读取</li>
 *   <li><b>Hybrid Shuffle</b>：结合内存和磁盘，支持 Tiered Storage</li>
 * </ul>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li><b>JobMaster</b>：ShuffleMaster 由 JobMaster 持有，在作业调度时调用</li>
 *   <li><b>ShuffleEnvironment</b>：运行在 TaskManager 端，负责实际的数据读写</li>
 *   <li><b>TieredInternalShuffleMaster</b>：可选的分层存储支持，处理远程存储分区</li>
 * </ul>
 *
 * <h3>配置参数</h3>
 * <ul>
 *   <li>{@code taskmanager.network.memory.buffers-per-channel}：每个 Channel 的缓冲区数</li>
 *   <li>{@code taskmanager.network.memory.floating-buffers-per-gate}：每个 Gate 的浮动缓冲区数</li>
 *   <li>{@code taskmanager.network.sort-shuffle.min-buffers}：Sort Shuffle 最小缓冲区数</li>
 * </ul>
 */
public class NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {

    /**
     * 每个 InputChannel 的独占缓冲区数量。
     * 这些缓冲区预分配给特定的 Channel，用于接收上游数据。
     */
    private final int buffersPerInputChannel;

    /**
     * 每个 InputGate 的浮动缓冲区数量。
     * 浮动缓冲区在多个 Channel 之间共享，动态分配给需要的 Channel。
     */
    private final int floatingBuffersPerGate;

    /**
     * 每个 Gate 所需的最大缓冲区数（可选配置）。
     * 用于限制内存使用，防止单个 Gate 占用过多缓冲区。
     */
    private final Optional<Integer> maxRequiredBuffersPerGate;

    /**
     * 启用 Sort Shuffle 的最小并行度阈值。
     * 当下游任务并行度超过此值时，优先使用 Sort-Merge Shuffle 而非 Hash Shuffle。
     */
    private final int sortShuffleMinParallelism;

    /**
     * Sort Shuffle 所需的最小缓冲区数。
     * Sort-Merge Shuffle 需要足够的缓冲区来进行内存排序。
     */
    private final int sortShuffleMinBuffers;

    /**
     * 网络缓冲区大小（字节）。
     * 默认 32KB，与 MemorySegment 大小一致。
     */
    private final int networkBufferSize;

    /**
     * 是否启用 JobMaster Failover 支持。
     * 启用后会在 Checkpoint 时保存 Shuffle 状态，支持 JobMaster 故障恢复。
     */
    private final boolean enableJobMasterFailover;

    /**
     * 分层存储 Shuffle 主控（可选）。
     * 当配置为 Hybrid Shuffle 模式时创建，管理远程存储（如 HDFS、S3）上的分区数据。
     */
    @Nullable private final TieredInternalShuffleMaster tieredInternalShuffleMaster;

    /**
     * 作业级别的 Shuffle 上下文映射。
     * Key: JobID，Value: JobShuffleContext，用于查询分区信息和度量数据。
     */
    private final Map<JobID, JobShuffleContext> jobShuffleContexts = new HashMap<>();

    /**
     * 作业的 Shuffle 描述符缓存。
     * 仅在启用 JobMaster Failover 时使用，用于快照和恢复分区元数据。
     * Key: JobID，Value: 分区 ID 到 ShuffleDescriptor 的映射。
     */
    private final Map<JobID, Map<ResultPartitionID, ShuffleDescriptor>> jobShuffleDescriptors =
            new HashMap<>();

    /**
     * 构造 NettyShuffleMaster 实例。
     *
     * <p>初始化过程：
     * <ol>
     *   <li>从配置中读取网络缓冲区相关参数</li>
     *   <li>如果配置了 Hybrid Shuffle 模式，创建 TieredInternalShuffleMaster</li>
     *   <li>根据配置决定是否启用 JobMaster Failover 支持</li>
     * </ol>
     *
     * @param shuffleMasterContext Shuffle 主控上下文，包含配置信息
     */
    public NettyShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
        Configuration conf = shuffleMasterContext.getConfiguration();
        checkNotNull(conf);
        // 注意：buffersPerInputChannel 和 floatingBuffersPerGate 使用硬编码值
        // 实际 TM 端会从配置读取，这里仅用于内存计算
        buffersPerInputChannel = 2;
        floatingBuffersPerGate = 8;
        maxRequiredBuffersPerGate =
                conf.getOptional(
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE);
        sortShuffleMinParallelism = 1;
        sortShuffleMinBuffers =
                conf.get(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        networkBufferSize = ConfigurationParserUtils.getPageSize(conf);

        // 根据 Shuffle 模式决定是否创建分层存储支持
        if (isHybridShuffleEnabled(conf)) {
            tieredInternalShuffleMaster =
                    new TieredInternalShuffleMaster(
                            shuffleMasterContext, this::getShuffleDescriptor);
        } else {
            tieredInternalShuffleMaster = null;
        }

        // 启用 Failover 需要同时满足配置开启和支持批快照
        enableJobMasterFailover =
                conf.get(BatchExecutionOptions.JOB_RECOVERY_ENABLED) && supportsBatchSnapshot();

        // 参数校验：每个 Gate 至少需要 1 个缓冲区
        checkArgument(
                !maxRequiredBuffersPerGate.isPresent() || maxRequiredBuffersPerGate.get() >= 1,
                String.format(
                        "At least one buffer is required for each gate, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE
                                .key()));
    }

    /**
     * 向生产者注册分区并返回 ShuffleDescriptor。
     *
     * <p>此方法是 Shuffle 主控的核心功能，在 Task 部署时由 JobMaster 调用。
     * 生成的 ShuffleDescriptor 会被传递给下游消费者，用于定位和连接数据源。
     *
     * <p>处理流程：
     * <ol>
     *   <li>根据分区 ID 和执行 ID 构建 ResultPartitionID</li>
     *   <li>如果启用了分层存储，向 TieredInternalShuffleMaster 注册并获取 TierShuffleDescriptor</li>
     *   <li>创建 NettyShuffleDescriptor，包含生产者位置和连接信息</li>
     *   <li>如果启用 Failover，缓存 ShuffleDescriptor 供状态恢复使用</li>
     * </ol>
     *
     * @param jobID 作业 ID
     * @param partitionDescriptor 分区描述，包含分区 ID、子分区数量等
     * @param producerDescriptor 生产者描述，包含位置信息、数据端口等
     * @return 包含 NettyShuffleDescriptor 的 CompletableFuture
     */
    @Override
    public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {

        // 构建分区的唯一标识
        ResultPartitionID resultPartitionID =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());

        // 处理分层存储（如果启用）
        List<TierShuffleDescriptor> tierShuffleDescriptors = null;
        if (tieredInternalShuffleMaster != null) {
            tierShuffleDescriptors =
                    tieredInternalShuffleMaster.addPartitionAndGetShuffleDescriptor(
                            jobID,
                            partitionDescriptor.getNumberOfSubpartitions(),
                            resultPartitionID);
        }

        // 创建 Shuffle 部署描述符
        NettyShuffleDescriptor shuffleDeploymentDescriptor =
                new NettyShuffleDescriptor(
                        producerDescriptor.getProducerLocation(),
                        createConnectionInfo(
                                producerDescriptor, partitionDescriptor.getConnectionIndex()),
                        resultPartitionID,
                        tierShuffleDescriptors);

        // 缓存描述符用于 Failover 恢复
        if (enableJobMasterFailover) {
            Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptorMap =
                    jobShuffleDescriptors.computeIfAbsent(jobID, k -> new HashMap<>());
            shuffleDescriptorMap.put(resultPartitionID, shuffleDeploymentDescriptor);
        }
        return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
    }

    /**
     * 释放外部分区资源。
     *
     * <p>当分区不再需要时（如 Task 失败重启、作业取消），释放相关资源。
     * 对于分层存储，需要通知远程存储清理数据。
     */
    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.releasePartition(shuffleDescriptor);
        }
    }

    /**
     * 根据作业 ID 和分区 ID 获取 ShuffleDescriptor。
     * 主要用于 Failover 恢复和分层存储查询。
     */
    public Optional<ShuffleDescriptor> getShuffleDescriptor(
            JobID jobID, ResultPartitionID resultPartitionID) {
        return Optional.ofNullable(jobShuffleDescriptors.get(jobID))
                .map(descriptorMap -> descriptorMap.get(resultPartitionID));
    }

    /**
     * 根据生产者信息创建分区连接信息。
     *
     * <p>如果生产者有有效的数据端口（>= 0），创建网络连接信息；
     * 否则使用本地执行连接信息（本地模式或同 TM 内的数据交换）。
     */
    private static PartitionConnectionInfo createConnectionInfo(
            ProducerDescriptor producerDescriptor, int connectionIndex) {
        return producerDescriptor.getDataPort() >= 0
                ? NetworkPartitionConnectionInfo.fromProducerDescriptor(
                        producerDescriptor, connectionIndex)
                : LocalExecutionPartitionConnectionInfo.INSTANCE;
    }

    /**
     * JM announces network memory requirement from the calculating result of this method. Please
     * note that the calculating algorithm depends on both I/O details of a vertex and network
     * configuration, which means we should always keep the consistency of configurations between
     * JM, RM and TM in fine-grained resource management, thus to guarantee that the processes of
     * memory announcing and allocating respect each other.
     *
     * <p>计算 Task 所需的 Shuffle 内存大小。
     *
     * <p>此方法由 JobMaster 在细粒度资源管理模式下调用，计算结果会通知给 ResourceManager
     * 用于资源分配决策。计算考虑以下因素：
     * <ul>
     *   <li>输入 Channel 数量和每个 Channel 的缓冲区配置</li>
     *   <li>输出子分区数量和分区类型</li>
     *   <li>分区复用计数</li>
     *   <li>是否使用 Sort Shuffle</li>
     * </ul>
     *
     * <p><b>重要</b>：JM、RM、TM 三端的配置必须保持一致，否则会导致内存分配与实际需求不匹配。
     */
    @Override
    public MemorySize computeShuffleMemorySizeForTask(TaskInputsOutputsDescriptor desc) {
        checkNotNull(desc);

        // 根据 Task 的输入输出特征计算所需缓冲区数量
        int numRequiredNetworkBuffers =
                NettyShuffleUtils.computeNetworkBuffersForAnnouncing(
                        buffersPerInputChannel,
                        floatingBuffersPerGate,
                        maxRequiredBuffersPerGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        desc.getInputChannelNums(),
                        desc.getPartitionReuseCount(),
                        desc.getSubpartitionNums(),
                        desc.getInputPartitionTypes(),
                        desc.getPartitionTypes());

        // 缓冲区数量 × 缓冲区大小 = 总内存需求
        return new MemorySize((long) networkBufferSize * numRequiredNetworkBuffers);
    }

    /**
     * 检查是否启用了 Hybrid Shuffle 模式。
     * Hybrid Shuffle 结合内存和远程存储，支持更灵活的数据交换策略。
     */
    private boolean isHybridShuffleEnabled(Configuration conf) {
        return (conf.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_FULL
                || conf.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_SELECTIVE);
    }

    /**
     * 获取分区及其度量信息。
     * 用于投机执行和资源调度决策，了解分区数据的可用性和大小。
     */
    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        if (tieredInternalShuffleMaster != null) {
            return tieredInternalShuffleMaster.getPartitionWithMetrics(
                    jobShuffleContexts.get(jobId), timeout, expectedPartitions);
        }

        return checkNotNull(jobShuffleContexts.get(jobId))
                .getPartitionWithMetrics(timeout, expectedPartitions);
    }

    /**
     * 注册作业到 Shuffle 主控。
     * 在作业启动时调用，建立作业与 Shuffle 服务的关联。
     */
    @Override
    public void registerJob(JobShuffleContext context) {
        jobShuffleContexts.put(context.getJobId(), context);
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.registerJob(context);
        }
    }

    /**
     * 注销作业。
     * 在作业完成或取消时调用，清理相关资源和状态。
     */
    @Override
    public void unregisterJob(JobID jobId) {
        jobShuffleContexts.remove(jobId);
        if (tieredInternalShuffleMaster != null) {
            if (enableJobMasterFailover) {
                jobShuffleDescriptors.remove(jobId);
            }
            tieredInternalShuffleMaster.unregisterJob(jobId);
        }
    }

    @Override
    public boolean supportsBatchSnapshot() {
        if (tieredInternalShuffleMaster != null) {
            return tieredInternalShuffleMaster.supportsBatchSnapshot();
        }

        return true;
    }

    @Override
    public void snapshotState(
            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {
        if (tieredInternalShuffleMaster != null) {
            Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptorMap =
                    jobShuffleDescriptors.remove(jobId);
            CompletableFuture<AllTieredShuffleMasterSnapshots> allSnapshotFuture =
                    new CompletableFuture<>();
            tieredInternalShuffleMaster.snapshotState(allSnapshotFuture, context, jobId);
            allSnapshotFuture.thenAccept(
                    allSnap ->
                            snapshotFuture.complete(
                                    new TieredInternalShuffleMasterSnapshot(
                                            shuffleDescriptorMap, allSnap)));
            return;
        }

        snapshotFuture.complete(EmptyShuffleMasterSnapshot.getInstance());
    }

    @Override
    public void snapshotState(CompletableFuture<ShuffleMasterSnapshot> snapshotFuture) {
        if (tieredInternalShuffleMaster != null) {
            CompletableFuture<AllTieredShuffleMasterSnapshots> allSnapshotFuture =
                    new CompletableFuture<>();
            tieredInternalShuffleMaster.snapshotState(allSnapshotFuture);
            allSnapshotFuture.thenAccept(
                    allSnap ->
                            snapshotFuture.complete(
                                    new TieredInternalShuffleMasterSnapshot(null, allSnap)));
            return;
        }

        snapshotFuture.complete(EmptyShuffleMasterSnapshot.getInstance());
    }

    @Override
    public void restoreState(ShuffleMasterSnapshot snapshot) {
        if (tieredInternalShuffleMaster != null) {
            checkState(snapshot instanceof TieredInternalShuffleMasterSnapshot);
            tieredInternalShuffleMaster.restoreState(
                    (TieredInternalShuffleMasterSnapshot) snapshot);
        }
    }

    @Override
    public void restoreState(List<ShuffleMasterSnapshot> snapshots, JobID jobId) {
        if (tieredInternalShuffleMaster != null) {
            List<TieredInternalShuffleMasterSnapshot> snapshotList =
                    snapshots.stream()
                            .map(
                                    snap -> {
                                        checkState(
                                                snap
                                                        instanceof
                                                        TieredInternalShuffleMasterSnapshot);
                                        Map<ResultPartitionID, ShuffleDescriptor>
                                                shuffleDescriptors =
                                                        ((TieredInternalShuffleMasterSnapshot) snap)
                                                                .getShuffleDescriptors();
                                        if (shuffleDescriptors != null) {
                                            jobShuffleDescriptors
                                                    .computeIfAbsent(jobId, k -> new HashMap<>())
                                                    .putAll(shuffleDescriptors);
                                        }
                                        return (TieredInternalShuffleMasterSnapshot) snap;
                                    })
                            .collect(Collectors.toList());
            tieredInternalShuffleMaster.restoreState(snapshotList, jobId);
        }
    }

    @Override
    public void notifyPartitionRecoveryStarted(JobID jobId) {
        checkNotNull(jobShuffleContexts.get(jobId)).notifyPartitionRecoveryStarted();
    }

    @Override
    public void close() throws Exception {
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.close();
        }
    }
}
