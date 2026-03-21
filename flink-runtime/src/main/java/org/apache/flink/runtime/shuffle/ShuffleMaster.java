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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Intermediate result partition registry to use in {@link
 * org.apache.flink.runtime.jobmaster.JobMaster}.
 *
 * <p>【学习型注释】ShuffleMaster 是 Shuffle 服务的主控接口，运行在 JobMaster 端，
 * 负责管理作业的所有数据分区元信息。
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li>注册分区：当 ExecutionVertex 部署时，注册其输出分区</li>
 *   <li>生成描述符：为每个分区生成 ShuffleDescriptor，供下游消费者连接</li>
 *   <li>释放分区：当作业完成或分区不再需要时释放资源</li>
 *   <li>快照/恢复：支持批处理作业的容错恢复</li>
 * </ul>
 *
 * <h2>实现类</h2>
 * <ul>
 *   <li>{@link NettyShuffleMaster}：基于 Netty 的默认实现</li>
 *   <li>External Shuffle Service 的实现（如 YARN、Kubernetes 等）</li>
 * </ul>
 *
 * <h2>工作流程</h2>
 * <pre>
 * 1. JobMaster 启动 ShuffleMaster
 * 2. Task 部署时调用 registerPartitionWithProducer() 注册分区
 * 3. ShuffleMaster 返回 ShuffleDescriptor
 * 4. JobMaster 将描述符传递给下游消费者
 * 5. 作业结束时调用 releasePartitionExternally() 清理资源
 * </pre>
 *
 * <h2>与 ShuffleEnvironment 的关系</h2>
 * <ul>
 *   <li>ShuffleMaster 运行在 JobMaster 端，管理全局元信息</li>
 *   <li>ShuffleEnvironment 运行在 TaskManager 端，管理本地数据传输</li>
 * </ul>
 *
 * @param <T> partition shuffle descriptor used for producer/consumer deployment and their data
 *     exchange.
 */
public interface ShuffleMaster<T extends ShuffleDescriptor> extends AutoCloseable {

    /**
     * Starts this shuffle master as a service. One can do some initialization here, for example
     * getting access and connecting to the external system.
     *
     * <p>【学习型注释】启动 ShuffleMaster 服务。可进行初始化工作，如连接外部 Shuffle 服务。
     */
    default void start() throws Exception {}

    /**
     * Closes this shuffle master service which should release all resources. A shuffle master will
     * only be closed when the cluster is shut down.
     *
     * <p>【学习型注释】关闭 ShuffleMaster 服务，释放所有资源。仅在集群关闭时调用。
     */
    @Override
    default void close() throws Exception {}

    /**
     * Registers the target job together with the corresponding {@link JobShuffleContext} to this
     * shuffle master. Through the shuffle context, one can obtain some basic information like job
     * ID, job configuration. It enables ShuffleMaster to notify JobMaster about lost result
     * partitions, so that JobMaster can identify and reproduce unavailable partitions earlier.
     *
     * @param context the corresponding shuffle context of the target job.
     */
    default void registerJob(JobShuffleContext context) {}

    /**
     * Unregisters the target job from this shuffle master, which means the corresponding job has
     * reached a global termination state and all the allocated resources except for the cluster
     * partitions can be cleared.
     *
     * @param jobID ID of the target job to be unregistered.
     */
    default void unregisterJob(JobID jobID) {}

    /**
     * Asynchronously register a partition and its producer with the shuffle service.
     *
     * <p>The returned shuffle descriptor is an internal handle which identifies the partition
     * internally within the shuffle service. The descriptor should provide enough information to
     * read from or write data to the partition.
     *
     * <p>【学习型注释】异步注册分区及其生产者。这是 ShuffleMaster 最核心的方法。
     *
     * <p>调用时机：ExecutionVertex 部署到 TaskManager 之前
     *
     * <p>参数说明：
     * <ul>
     *   <li>jobID：作业 ID，用于按作业隔离分区</li>
     *   <li>partitionDescriptor：分区描述（分区 ID、类型、子分区数等）</li>
     *   <li>producerDescriptor：生产者描述（位置、执行 ID、连接信息）</li>
     * </ul>
     *
     * <p>返回值：包含连接信息的 ShuffleDescriptor，下游消费者据此建立连接
     *
     * @param jobID job ID of the corresponding job which registered the partition
     * @param partitionDescriptor general job graph information about the partition
     * @param producerDescriptor general producer information (location, execution id, connection
     *     info)
     * @return future with the partition shuffle descriptor used for producer/consumer deployment
     *     and their data exchange.
     */
    CompletableFuture<T> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor);

    /**
     * Release any external resources occupied by the given partition.
     *
     * <p>This call triggers release of any resources which are occupied by the given partition in
     * the external systems outside of the producer executor. This is mostly relevant for the batch
     * jobs and blocking result partitions. The producer local resources are managed by {@link
     * ShuffleDescriptor#storesLocalResourcesOn()} and {@link
     * ShuffleEnvironment#releasePartitionsLocally(Collection)}.
     *
     * @param shuffleDescriptor shuffle descriptor of the result partition to release externally.
     */
    void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor);

    /**
     * Compute shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
     *
     * @param taskInputsOutputsDescriptor describes task inputs and outputs information for shuffle
     *     memory calculation.
     * @return shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
     */
    default MemorySize computeShuffleMemorySizeForTask(
            TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
        return MemorySize.ZERO;
    }

    /**
     * Retrieves specified partitions and their metrics (identified by {@code expectedPartitions}),
     * the metrics include sizes of sub-partitions in a result partition.
     *
     * @param jobId ID of the target job
     * @param timeout The timeout used for retrieve the specified partitions.
     * @param expectedPartitions The set of identifiers for the result partitions whose metrics are
     *     to be fetched.
     * @return A future will contain a collection of the partitions with their metrics that could be
     *     retrieved from the expected partitions within the specified timeout period.
     */
    default CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    /**
     * Whether the shuffle master supports taking snapshot in batch scenarios if {@link
     * org.apache.flink.configuration.BatchExecutionOptions#JOB_RECOVERY_ENABLED} is true. If it
     * returns true, Flink will call {@link #snapshotState} to take snapshot, and call {@link
     * #restoreState} to restore the state of shuffle master.
     */
    default boolean supportsBatchSnapshot() {
        return false;
    }

    /** Triggers a snapshot of the shuffle master's state. */
    default void snapshotState(CompletableFuture<ShuffleMasterSnapshot> snapshotFuture) {}

    /** Triggers a snapshot of the shuffle master's state which related the specified job. */
    default void snapshotState(
            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {}

    /** Restores the state of the shuffle master from the provided snapshots. */
    default void restoreState(ShuffleMasterSnapshot snapshot) {}

    /**
     * Restores the state of the shuffle master from the provided snapshots for the specified job.
     */
    default void restoreState(List<ShuffleMasterSnapshot> snapshots, JobID jobId) {}

    /**
     * Notifies that the recovery process of result partitions has started.
     *
     * @param jobId ID of the target job
     */
    default void notifyPartitionRecoveryStarted(JobID jobId) {}
}
