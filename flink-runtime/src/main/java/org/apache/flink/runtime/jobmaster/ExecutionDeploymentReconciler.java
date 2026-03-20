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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;

import java.util.Map;

/**
 * Component for reconciling the deployment state of executions.
 *
 * <p>【学习型注释】
 * ExecutionDeploymentReconciler 用于协调 JobMaster 和 TaskExecutor 之间的 Execution 部署状态。
 * 在分布式环境下，由于网络延迟、节点故障等原因，双方对 Task 部署状态的认知可能出现不一致。
 * 该组件通过对比期望状态与实际状态，发现并解决以下问题：
 * - 孤儿任务：TaskExecutor 上有 JobMaster 不知道的 Task 在运行
 * - 丢失部署：JobMaster 认为应该运行的 Task 在 TaskExecutor 上不存在
 * 通过定期协调，确保作业执行状态的最终一致性。
 */
public interface ExecutionDeploymentReconciler {

    /** Factory for {@link ExecutionDeploymentReconciler}. */
    interface Factory {
        ExecutionDeploymentReconciler create(
                ExecutionDeploymentReconciliationHandler reconciliationHandler);
    }

    /**
     * Reconciles the deployment states between all reported/expected executions for the given task
     * executor.
     *
     * @param taskExecutorHost hosting task executor
     * @param executionDeploymentReport task executor report for deployed executions
     * @param expectedDeployedExecutionIds map of expected executions and their current deployment
     *     status
     */
    void reconcileExecutionDeployments(
            ResourceID taskExecutorHost,
            ExecutionDeploymentReport executionDeploymentReport,
            Map<ExecutionAttemptID, ExecutionDeploymentState> expectedDeployedExecutionIds);
}
