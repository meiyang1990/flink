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

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.Set;

/** New interface for failover strategies. */
public interface FailoverStrategy {

    /** 【学习型注释】
     * FailoverStrategy 定义了 Flink 作业失败时的恢复策略接口。
     *
     * <p>核心职责：
     * 当某个 Task 执行失败时，决定需要重启哪些相关的 Task。
     * 不同的策略会影响故障恢复的范围和效率。
     *
     * <p>实现类：
     * - RestartPipelinedRegionStrategy：重启失败Task所在的整个Pipelined区域（流式作业默认）
     * - FullRecoveryStrategy：故障时重启所有Task（批处理场景）
     * - RegionFailoverStrategy：基于Region的故障恢复，优化重启范围
     *
     * <p>设计考量：
     * 故障恢复策略需要在「重启范围小（快速恢复）」和「数据一致性（正确性）」之间权衡。
     * 对于流式作业，通常采用 Pipeline 边连接的区域作为恢复单元。
     */

    /**
     * Returns a set of IDs corresponding to the set of vertices that should be restarted.
     *
     * @param executionVertexId ID of the failed task
     * @param cause cause of the failure
     * @return set of IDs of vertices to restart
     */
    Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID executionVertexId, Throwable cause);

    // ------------------------------------------------------------------------
    //  factory
    // ------------------------------------------------------------------------

    /** The factory to instantiate {@link FailoverStrategy}. */
    interface Factory {

        /**
         * Instantiates the {@link FailoverStrategy}.
         *
         * @param topology of the graph to failover
         * @param resultPartitionAvailabilityChecker to check whether a result partition is
         *     available
         * @return The instantiated failover strategy.
         */
        FailoverStrategy create(
                SchedulingTopology topology,
                ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker);
    }
}
