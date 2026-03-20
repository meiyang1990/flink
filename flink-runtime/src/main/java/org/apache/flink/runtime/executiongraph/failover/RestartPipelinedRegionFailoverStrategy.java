/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that proposes to restart involved regions when a vertex fails. A region is
 * defined by this strategy as tasks that communicate via pipelined data exchange.
 *
 * <p>【学习型注释】
 * RestartPipelinedRegionFailoverStrategy 是 Flink 流式作业的默认故障恢复策略。
 * 当某个 Task 失败时，它会找出需要重启的最小 Task 集合，而非重启整个作业。
 *
 * <p>核心概念 - Pipelined Region：
 * 由通过 PIPELINED 边（流式数据交换）连接的一组 Task 组成。
 * 同一 Region 内的 Task 必须同时运行，因为数据是流式传递的，不会持久化。
 *
 * <p>恢复规则：
 * 1. 失败 Task 所在的 Region 必须重启
 * 2. 如果某个 Region 的输入分区不可用（丢失或损坏），生产该分区的 Region 也需重启
 * 3. 如果某个 Region 需要重启，它的所有下游消费者 Region 也必须重启
 *
 * <p>与 Checkpoint 的配合：
 * 重启后，Task 会从最近成功的 Checkpoint 恢复状态，保证 Exactly-Once 语义。
 *
 * <p>优势：
 * 相比全量重启（RestartAllStrategy），Region 级别恢复可以显著减少故障影响范围，
 * 特别是在大规模作业中，只需重启失败相关的部分 Task。
 */
public class RestartPipelinedRegionFailoverStrategy implements FailoverStrategy {

    /** The topology containing info about all the vertices and result partitions. */
    private final SchedulingTopology topology;

    /** The checker helps to query result partition availability. */
    private final RegionFailoverResultPartitionAvailabilityChecker
            resultPartitionAvailabilityChecker;

    /**
     * Creates a new failover strategy to restart pipelined regions that works on the given
     * topology. The result partitions are always considered to be available if no data consumption
     * error happens.
     *
     * @param topology containing info about all the vertices and result partitions
     */
    @VisibleForTesting
    public RestartPipelinedRegionFailoverStrategy(SchedulingTopology topology) {
        this(topology, resultPartitionID -> true);
    }

    /**
     * Creates a new failover strategy to restart pipelined regions that works on the given
     * topology.
     *
     * @param topology containing info about all the vertices and result partitions
     * @param resultPartitionAvailabilityChecker helps to query result partition availability
     */
    public RestartPipelinedRegionFailoverStrategy(
            SchedulingTopology topology,
            ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

        this.topology = checkNotNull(topology);
        this.resultPartitionAvailabilityChecker =
                new RegionFailoverResultPartitionAvailabilityChecker(
                        resultPartitionAvailabilityChecker,
                        (intermediateResultPartitionID ->
                                topology.getResultPartition(intermediateResultPartitionID)
                                        .getResultType()));
    }

    // ------------------------------------------------------------------------
    //  task failure handling
    // ------------------------------------------------------------------------

    /**
     * Returns a set of IDs corresponding to the set of vertices that should be restarted. In this
     * strategy, all task vertices in 'involved' regions are proposed to be restarted. The
     * 'involved' regions are calculated with rules below: 1. The region containing the failed task
     * is always involved 2. If an input result partition of an involved region is not available,
     * i.e. Missing or Corrupted, the region containing the partition producer task is involved 3.
     * If a region is involved, all of its consumer regions are involved
     *
     * <p>【注释】获取失败后需要重启的 Task 集合。
     * 计算逻辑：先定位失败 Task 所在的 Region，然后根据数据依赖关系递归扩展到所有受影响的 Region。
     *
     * @param executionVertexId ID of the failed task
     * @param cause cause of the failure
     * @return set of IDs of vertices to restart
     */
    @Override
    public Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID executionVertexId, Throwable cause) {

        final SchedulingPipelinedRegion failedRegion =
                topology.getPipelinedRegionOfVertex(executionVertexId);
        if (failedRegion == null) {
            // TODO: show the task name in the log
            throw new IllegalStateException(
                    "Can not find the failover region for task " + executionVertexId, cause);
        }

        // if the failure cause is data consumption error, mark the corresponding data partition to
        // be failed,
        // so that the failover process will try to recover it
        Optional<PartitionException> dataConsumptionException =
                ExceptionUtils.findThrowable(cause, PartitionException.class);
        if (dataConsumptionException.isPresent()) {
            resultPartitionAvailabilityChecker.markResultPartitionFailed(
                    dataConsumptionException.get().getPartitionId().getPartitionId());
        }

        // calculate the tasks to restart based on the result of regions to restart
        Set<ExecutionVertexID> tasksToRestart = new HashSet<>();
        for (SchedulingPipelinedRegion region : getRegionsToRestart(failedRegion)) {
            for (SchedulingExecutionVertex vertex : region.getVertices()) {
                // we do not need to restart tasks which are already in the initial state
                if (vertex.getState() != ExecutionState.CREATED) {
                    tasksToRestart.add(vertex.getId());
                }
            }
        }

        // the previous failed partition will be recovered. remove its failed state from the checker
        if (dataConsumptionException.isPresent()) {
            resultPartitionAvailabilityChecker.removeResultPartitionFromFailedState(
                    dataConsumptionException.get().getPartitionId().getPartitionId());
        }

        return tasksToRestart;
    }

    /**
     * All 'involved' regions are proposed to be restarted. The 'involved' regions are calculated
     * with rules below: 1. The region containing the failed task is always involved 2. If an input
     * result partition of an involved region is not available, i.e. Missing or Corrupted, the
     * region containing the partition producer task is involved 3. If a region is involved, all of
     * its consumer regions are involved
     *
     * <p>【注释】使用 BFS 算法遍历所有受影响的 Region：
     * 1. 从失败 Region 开始入队
     * 2. 对于每个 Region，检查其输入分区是否可用，不可用则将生产者 Region 加入队列
     * 3. 将当前 Region 的所有消费者 Region 也加入队列
     * 4. 最终返回所有需要重启的 Region 集合
     */
    private Set<SchedulingPipelinedRegion> getRegionsToRestart(
            SchedulingPipelinedRegion failedRegion) {
        Set<SchedulingPipelinedRegion> regionsToRestart =
                Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SchedulingPipelinedRegion> visitedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());

        Set<ConsumedPartitionGroup> visitedConsumedResultGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());
        Set<ConsumerVertexGroup> visitedConsumerVertexGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // start from the failed region to visit all involved regions
        Queue<SchedulingPipelinedRegion> regionsToVisit = new ArrayDeque<>();
        visitedRegions.add(failedRegion);
        regionsToVisit.add(failedRegion);
        while (!regionsToVisit.isEmpty()) {
            SchedulingPipelinedRegion regionToRestart = regionsToVisit.poll();

            // an involved region should be restarted
            regionsToRestart.add(regionToRestart);

            // if a needed input result partition is not available, its producer region is involved
            for (IntermediateResultPartitionID consumedPartitionId :
                    getConsumedPartitionsToVisit(regionToRestart, visitedConsumedResultGroups)) {
                if (!resultPartitionAvailabilityChecker.isAvailable(consumedPartitionId)) {
                    SchedulingResultPartition consumedPartition =
                            topology.getResultPartition(consumedPartitionId);
                    SchedulingPipelinedRegion producerRegion =
                            topology.getPipelinedRegionOfVertex(
                                    consumedPartition.getProducer().getId());
                    if (!visitedRegions.contains(producerRegion)) {
                        visitedRegions.add(producerRegion);
                        regionsToVisit.add(producerRegion);
                    }
                }
            }

            // all consumer regions of an involved region should be involved
            for (ExecutionVertexID consumerVertexId :
                    getConsumerVerticesToVisit(regionToRestart, visitedConsumerVertexGroups)) {
                SchedulingPipelinedRegion consumerRegion =
                        topology.getPipelinedRegionOfVertex(consumerVertexId);
                if (!visitedRegions.contains(consumerRegion)) {
                    visitedRegions.add(consumerRegion);
                    regionsToVisit.add(consumerRegion);
                }
            }
        }

        return regionsToRestart;
    }

    private Iterable<IntermediateResultPartitionID> getConsumedPartitionsToVisit(
            SchedulingPipelinedRegion regionToRestart,
            Set<ConsumedPartitionGroup> visitedConsumedResultGroups) {

        final List<ConsumedPartitionGroup> consumedPartitionGroupsToVisit = new ArrayList<>();

        for (SchedulingExecutionVertex vertex : regionToRestart.getVertices()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    vertex.getConsumedPartitionGroups()) {
                if (!visitedConsumedResultGroups.contains(consumedPartitionGroup)) {
                    visitedConsumedResultGroups.add(consumedPartitionGroup);
                    consumedPartitionGroupsToVisit.add(consumedPartitionGroup);
                }
            }
        }

        return IterableUtils.flatMap(consumedPartitionGroupsToVisit, Function.identity());
    }

    private Iterable<ExecutionVertexID> getConsumerVerticesToVisit(
            SchedulingPipelinedRegion regionToRestart,
            Set<ConsumerVertexGroup> visitedConsumerVertexGroups) {
        final List<ConsumerVertexGroup> consumerVertexGroupsToVisit = new ArrayList<>();

        for (SchedulingExecutionVertex vertex : regionToRestart.getVertices()) {
            for (SchedulingResultPartition producedPartition : vertex.getProducedResults()) {
                for (ConsumerVertexGroup consumerVertexGroup :
                        producedPartition.getConsumerVertexGroups()) {
                    if (!visitedConsumerVertexGroups.contains(consumerVertexGroup)) {
                        visitedConsumerVertexGroups.add(consumerVertexGroup);
                        consumerVertexGroupsToVisit.add(consumerVertexGroup);
                    }
                }
            }
        }

        return IterableUtils.flatMap(consumerVertexGroupsToVisit, Function.identity());
    }

    // ------------------------------------------------------------------------
    //  testing
    // ------------------------------------------------------------------------

    /**
     * Returns the failover region that contains the given execution vertex.
     *
     * @return the failover region that contains the given execution vertex
     */
    @VisibleForTesting
    public SchedulingPipelinedRegion getFailoverRegion(ExecutionVertexID vertexID) {
        return topology.getPipelinedRegionOfVertex(vertexID);
    }

    /**
     * A stateful {@link ResultPartitionAvailabilityChecker} which maintains the failed partitions
     * which are not available.
     *
     * <p>【注释】有状态的分区可用性检查器。
     * 维护一个失败分区集合（failedPartitions），用于记录因 PartitionException 失败的分区。
     * 判断分区是否可用的条件：
     * 1. 分区未在失败列表中
     * 2. 通过 Shuffle Master 检查分区确实存在
     * 3. 分区类型支持重消费（isReconsumable）或是 PIPELINED_APPROXIMATE 类型
     */
    private static class RegionFailoverResultPartitionAvailabilityChecker
            implements ResultPartitionAvailabilityChecker {

        /** Result partition state checker from the shuffle master. */
        private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

        /** Records partitions which has caused {@link PartitionException}. */
        private final HashSet<IntermediateResultPartitionID> failedPartitions;

        /** Retrieve {@link ResultPartitionType} by {@link IntermediateResultPartitionID}. */
        private final Function<IntermediateResultPartitionID, ResultPartitionType>
                resultPartitionTypeRetriever;

        RegionFailoverResultPartitionAvailabilityChecker(
                ResultPartitionAvailabilityChecker checker,
                Function<IntermediateResultPartitionID, ResultPartitionType>
                        resultPartitionTypeRetriever) {
            this.resultPartitionAvailabilityChecker = checkNotNull(checker);
            this.failedPartitions = new HashSet<>();
            this.resultPartitionTypeRetriever = checkNotNull(resultPartitionTypeRetriever);
        }

        @Override
        public boolean isAvailable(IntermediateResultPartitionID resultPartitionID) {
            return !failedPartitions.contains(resultPartitionID)
                    && resultPartitionAvailabilityChecker.isAvailable(resultPartitionID)
                    // If the result partition is available in the partition tracker and does not
                    // fail, it will be available if it can be re-consumption, and it may also be
                    // available for PIPELINED_APPROXIMATE type.
                    && isResultPartitionIsReConsumableOrPipelinedApproximate(resultPartitionID);
        }

        public void markResultPartitionFailed(IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.add(resultPartitionID);
        }

        public void removeResultPartitionFromFailedState(
                IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.remove(resultPartitionID);
        }

        private boolean isResultPartitionIsReConsumableOrPipelinedApproximate(
                IntermediateResultPartitionID resultPartitionID) {
            ResultPartitionType resultPartitionType =
                    resultPartitionTypeRetriever.apply(resultPartitionID);
            return resultPartitionType.isReconsumable()
                    || resultPartitionType == ResultPartitionType.PIPELINED_APPROXIMATE;
        }
    }

    /** The factory to instantiate {@link RestartPipelinedRegionFailoverStrategy}. */
    public static class Factory implements FailoverStrategy.Factory {

        @Override
        public FailoverStrategy create(
                final SchedulingTopology topology,
                final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

            return new RestartPipelinedRegionFailoverStrategy(
                    topology, resultPartitionAvailabilityChecker);
        }
    }
}
