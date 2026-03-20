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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link ExecutionDeploymentReconciler} implementation. Detects missing/unknown
 * deployments, and defers to a provided {@link ExecutionDeploymentReconciliationHandler} to resolve
 * them.
 *
 * <p>【学习型注释】
 * DefaultExecutionDeploymentReconciler 是 ExecutionDeploymentReconciler 的默认实现，
 * 负责检测和处理 JobMaster 与 TaskExecutor 之间的部署状态不一致问题。
 * 核心逻辑：
 * - 对比 JobMaster 期望的部署状态（expectedDeployedExecutions）与 TaskExecutor 实际报告的状态（executionDeploymentReport）
 * - 识别两类不一致：
 *   1. unknownExecutions: TaskExecutor 报告了 JobMaster 不知道的 Execution（可能是孤儿任务）
 *   2. missingExecutions: JobMaster 期望部署但 TaskExecutor 未报告的 Execution（部署丢失）
 * - 通过 ExecutionDeploymentReconciliationHandler 回调处理这些不一致情况
 * 这种协调机制确保分布式环境下作业状态的一致性。
 */
public class DefaultExecutionDeploymentReconciler implements ExecutionDeploymentReconciler {

    private final ExecutionDeploymentReconciliationHandler handler;

    public DefaultExecutionDeploymentReconciler(ExecutionDeploymentReconciliationHandler handler) {
        this.handler = handler;
    }

    @Override
    public void reconcileExecutionDeployments(
            ResourceID taskExecutorHost,
            ExecutionDeploymentReport executionDeploymentReport,
            Map<ExecutionAttemptID, ExecutionDeploymentState> expectedDeployedExecutions) {
        final Set<ExecutionAttemptID> unknownExecutions =
                new HashSet<>(executionDeploymentReport.getExecutions());
        final Set<ExecutionAttemptID> missingExecutions = new HashSet<>();

        for (Map.Entry<ExecutionAttemptID, ExecutionDeploymentState> execution :
                expectedDeployedExecutions.entrySet()) {
            boolean deployed = unknownExecutions.remove(execution.getKey());
            if (!deployed && execution.getValue() != ExecutionDeploymentState.PENDING) {
                missingExecutions.add(execution.getKey());
            }
        }
        if (!unknownExecutions.isEmpty()) {
            handler.onUnknownDeploymentsOf(unknownExecutions, taskExecutorHost);
        }
        if (!missingExecutions.isEmpty()) {
            handler.onMissingDeploymentsOf(missingExecutions, taskExecutorHost);
        }
    }
}
