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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateID;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_INPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.createShuffleIOOwnerMetricGroup;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerDebloatingTaskMetrics;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerInputMetrics;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerOutputMetrics;
import static org.apache.flink.util.ExecutorUtils.gracefulShutdown;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link ShuffleEnvironment} based on netty network communication, local
 * memory and disk files. The network environment contains the data structures that keep track of
 * all intermediate results and shuffle data exchanges.
 *
 * <p>【学习型注释】
 * NettyShuffleEnvironment 是 Flink 基于 Netty 实现的 Shuffle 服务环境。
 * 负责管理 Task 之间的数据交换（Shuffle），是流式作业数据传输的核心组件。
 *
 * <p>核心职责：
 * 1. 管理网络缓冲池（NetworkBufferPool）：所有 Task 共享的内存缓冲区
 * 2. 管理结果分区（ResultPartition）：Task 输出数据的存储位置
 * 3. 管理输入门（InputGate）：Task 读取上游数据的入口
 * 4. 管理网络连接（ConnectionManager）：基于 Netty 的远程数据传输
 *
 * <p>数据交换模式：
 * - PIPELINED：流式模式，数据立即可消费（流式作业默认）
 * - BLOCKING：阻塞模式，数据写完后才可消费（批处理作业）
 * - HYBRID：混合模式，结合两者特点
 *
 * <p>网络缓冲管理：
 * NetworkBufferPool 预分配固定大小的内存缓冲区（taskmanager.memory.network.*）
 * ResultPartition 和 InputGate 从缓冲池申请/归还缓冲区
 * 缓冲区不足时触发背压（Backpressure）
 *
 * <p>关键配置：
 * - taskmanager.memory.network.fraction/min/max：网络缓冲内存配置
 * - taskmanager.network.memory.buffers-per-channel：每个通道的缓冲区数
 */
public class NettyShuffleEnvironment
        implements ShuffleEnvironment<ResultPartition, SingleInputGate> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleEnvironment.class);

    private final Object lock = new Object();

    /** 【注释】TaskExecutor 的资源 ID */
    private final ResourceID taskExecutorResourceId;

    /** 【注释】Shuffle 环境配置 */
    private final NettyShuffleEnvironmentConfiguration config;

    /** 【注释】网络缓冲池，所有 Task 共享的内存池，缓冲区不足会触发背压 */
    private final NetworkBufferPool networkBufferPool;

    /** 【注释】网络连接管理器，基于 Netty 实现远程数据传输 */
    private final ConnectionManager connectionManager;

    /** 【注释】结果分区管理器，管理所有 Task 的输出分区 */
    private final ResultPartitionManager resultPartitionManager;

    /** 【注释】文件通道管理器，用于溢写数据到磁盘（BLOCKING 模式） */
    private final FileChannelManager fileChannelManager;

    /** 【注释】InputGate ID 到 InputGate 集合的映射 */
    private final Map<InputGateID, Set<SingleInputGate>> inputGatesById;

    /** 【注释】结果分区工厂，创建 ResultPartition 实例 */
    private final ResultPartitionFactory resultPartitionFactory;

    /** 【注释】InputGate 工厂，创建 SingleInputGate 实例 */
    private final SingleInputGateFactory singleInputGateFactory;

    /** 【注释】IO 线程池执行器 */
    private final Executor ioExecutor;

    /** 【注释】批处理 Shuffle 读取缓冲池（专用于批处理作业） */
    private final BatchShuffleReadBufferPool batchShuffleReadBufferPool;

    /** 【注释】批处理 Shuffle 读取 IO 线程池 */
    private final ScheduledExecutorService batchShuffleReadIOExecutor;

    private boolean isClosed;

    NettyShuffleEnvironment(
            ResourceID taskExecutorResourceId,
            NettyShuffleEnvironmentConfiguration config,
            NetworkBufferPool networkBufferPool,
            ConnectionManager connectionManager,
            ResultPartitionManager resultPartitionManager,
            FileChannelManager fileChannelManager,
            ResultPartitionFactory resultPartitionFactory,
            SingleInputGateFactory singleInputGateFactory,
            Executor ioExecutor,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.config = config;
        this.networkBufferPool = networkBufferPool;
        this.connectionManager = connectionManager;
        this.resultPartitionManager = resultPartitionManager;
        this.inputGatesById = new ConcurrentHashMap<>(10);
        this.fileChannelManager = fileChannelManager;
        this.resultPartitionFactory = resultPartitionFactory;
        this.singleInputGateFactory = singleInputGateFactory;
        this.ioExecutor = ioExecutor;
        this.batchShuffleReadBufferPool = batchShuffleReadBufferPool;
        this.batchShuffleReadIOExecutor = batchShuffleReadIOExecutor;
        this.isClosed = false;
    }

    // --------------------------------------------------------------------------------------------
    //  Properties
    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public ResultPartitionManager getResultPartitionManager() {
        return resultPartitionManager;
    }

    @VisibleForTesting
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @VisibleForTesting
    public NetworkBufferPool getNetworkBufferPool() {
        return networkBufferPool;
    }

    @VisibleForTesting
    public BatchShuffleReadBufferPool getBatchShuffleReadBufferPool() {
        return batchShuffleReadBufferPool;
    }

    @VisibleForTesting
    public ScheduledExecutorService getBatchShuffleReadIOExecutor() {
        return batchShuffleReadIOExecutor;
    }

    @VisibleForTesting
    public NettyShuffleEnvironmentConfiguration getConfiguration() {
        return config;
    }

    @VisibleForTesting
    public Optional<Collection<SingleInputGate>> getInputGate(InputGateID id) {
        return Optional.ofNullable(inputGatesById.get(id));
    }

    @Override
    public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
        ioExecutor.execute(
                () -> {
                    for (ResultPartitionID partitionId : partitionIds) {
                        resultPartitionManager.releasePartition(partitionId, null);
                    }
                });
    }

    /**
     * Report unreleased partitions.
     *
     * @return collection of partitions which still occupy some resources locally on this task
     *     executor and have been not released yet.
     */
    @Override
    public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
        return resultPartitionManager.getUnreleasedPartitions();
    }

    @Override
    public Optional<ShuffleMetrics> getMetricsIfPartitionOccupyingLocalResource(
            ResultPartitionID partitionId) {
        return resultPartitionManager.getMetricsOfPartition(partitionId);
    }

    // --------------------------------------------------------------------------------------------
    //  Create Output Writers and Input Readers
    // --------------------------------------------------------------------------------------------

    @Override
    public ShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
        MetricGroup nettyGroup = createShuffleIOOwnerMetricGroup(checkNotNull(parentGroup));
        return new ShuffleIOOwnerContext(
                checkNotNull(ownerName),
                checkNotNull(executionAttemptID),
                parentGroup,
                nettyGroup.addGroup(METRIC_GROUP_OUTPUT),
                nettyGroup.addGroup(METRIC_GROUP_INPUT));
    }

    @Override
    public List<ResultPartition> createResultPartitionWriters(
            ShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            ResultPartition[] resultPartitions =
                    new ResultPartition[resultPartitionDeploymentDescriptors.size()];
            for (int partitionIndex = 0;
                    partitionIndex < resultPartitions.length;
                    partitionIndex++) {
                resultPartitions[partitionIndex] =
                        resultPartitionFactory.create(
                                ownerContext.getOwnerName(),
                                partitionIndex,
                                resultPartitionDeploymentDescriptors.get(partitionIndex));
            }

            registerOutputMetrics(
                    config.isNetworkDetailedMetrics(),
                    ownerContext.getOutputGroup(),
                    resultPartitions);
            return Arrays.asList(resultPartitions);
        }
    }

    @Override
    public List<SingleInputGate> createInputGates(
            ShuffleIOOwnerContext ownerContext,
            PartitionProducerStateProvider partitionProducerStateProvider,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            MetricGroup networkInputGroup = ownerContext.getInputGroup();

            InputChannelMetrics inputChannelMetrics =
                    new InputChannelMetrics(networkInputGroup, ownerContext.getParentGroup());

            SingleInputGate[] inputGates =
                    new SingleInputGate[inputGateDeploymentDescriptors.size()];
            for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
                final InputGateDeploymentDescriptor igdd =
                        inputGateDeploymentDescriptors.get(gateIndex);
                SingleInputGate inputGate =
                        singleInputGateFactory.create(
                                ownerContext,
                                gateIndex,
                                igdd,
                                partitionProducerStateProvider,
                                inputChannelMetrics);
                InputGateID id =
                        new InputGateID(
                                igdd.getConsumedResultId(), ownerContext.getExecutionAttemptID());
                Set<SingleInputGate> inputGateSet =
                        inputGatesById.computeIfAbsent(
                                id, ignored -> ConcurrentHashMap.newKeySet());
                inputGateSet.add(inputGate);
                inputGatesById.put(id, inputGateSet);
                inputGate
                        .getCloseFuture()
                        .thenRun(
                                () ->
                                        inputGatesById.computeIfPresent(
                                                id,
                                                (key, value) -> {
                                                    value.remove(inputGate);
                                                    if (value.isEmpty()) {
                                                        return null;
                                                    }
                                                    return value;
                                                }));
                inputGates[gateIndex] = inputGate;
            }

            if (config.getDebloatConfiguration().isEnabled()) {
                registerDebloatingTaskMetrics(inputGates, ownerContext.getParentGroup());
            }

            registerInputMetrics(config.isNetworkDetailedMetrics(), networkInputGroup, inputGates);
            return Arrays.asList(inputGates);
        }
    }

    @Override
    public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo)
            throws IOException, InterruptedException {
        IntermediateDataSetID intermediateResultPartitionID =
                partitionInfo.getIntermediateDataSetID();
        InputGateID id = new InputGateID(intermediateResultPartitionID, consumerID);
        Set<SingleInputGate> inputGates = inputGatesById.get(id);
        if (inputGates == null || inputGates.isEmpty()) {
            return false;
        }

        ShuffleDescriptor shuffleDescriptor = partitionInfo.getShuffleDescriptor();
        checkArgument(
                shuffleDescriptor instanceof NettyShuffleDescriptor,
                "Tried to update unknown channel with unknown ShuffleDescriptor %s.",
                shuffleDescriptor.getClass().getName());
        for (SingleInputGate inputGate : inputGates) {
            inputGate.updateInputChannel(
                    taskExecutorResourceId, (NettyShuffleDescriptor) shuffleDescriptor);
        }
        return true;
    }

    /*
     * Starts the internal related components for network connection and communication.
     *
     * @return a port to connect to the task executor for shuffle data exchange, -1 if only local connection is possible.
     */
    @Override
    public int start() throws IOException {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            LOG.info("Starting the network environment and its components.");

            try {
                LOG.debug("Starting network connection manager");
                return connectionManager.start();
            } catch (IOException t) {
                throw new IOException("Failed to instantiate network connection manager.", t);
            }
        }
    }

    /** Tries to shut down all network I/O components. */
    @Override
    public void close() {
        synchronized (lock) {
            if (isClosed) {
                return;
            }

            LOG.info("Shutting down the network environment and its components.");

            // terminate all network connections
            try {
                LOG.debug("Shutting down network connection manager");
                connectionManager.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down the network connection manager.", t);
            }

            // shutdown all intermediate results
            try {
                LOG.debug("Shutting down intermediate result partition manager");
                resultPartitionManager.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down the result partition manager.", t);
            }

            // make sure that the global buffer pool re-acquires all buffers
            try {
                networkBufferPool.destroyAllBufferPools();
            } catch (Throwable t) {
                LOG.warn("Could not destroy all buffer pools.", t);
            }

            // destroy the buffer pool
            try {
                networkBufferPool.destroy();
            } catch (Throwable t) {
                LOG.warn("Network buffer pool did not shut down properly.", t);
            }

            // delete all the temp directories
            try {
                fileChannelManager.close();
            } catch (Throwable t) {
                LOG.warn("Cannot close the file channel manager properly.", t);
            }

            try {
                gracefulShutdown(10, TimeUnit.SECONDS, batchShuffleReadIOExecutor);
            } catch (Throwable t) {
                LOG.warn("Cannot shut down batch shuffle read IO executor properly.", t);
            }

            try {
                batchShuffleReadBufferPool.destroy();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down batch shuffle read buffer pool properly.", t);
            }

            isClosed = true;
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return isClosed;
        }
    }
}
