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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default {@link ExecutionDeploymentTracker} implementation.
 *
 * <p>【学习型注释】
 * DefaultExecutionDeploymentTracker 是 ExecutionDeploymentTracker 的默认实现，
 * 用于跟踪 Task 部署状态。它维护了以下映射关系：
 * - pendingDeployments: 正在部署中的 Execution（尚未收到 TaskExecutor 确认）
 * - executionsByHost: 每个 TaskManager 上运行的 Execution 集合
 * - hostByExecution: 每个 Execution 所在的 TaskManager
 * 这些信息用于部署一致性检查（Deployment Reconciliation），确保 JobMaster 和 TaskExecutor 对 Task 状态达成一致。
 */
public class DefaultExecutionDeploymentTracker implements ExecutionDeploymentTracker {

    private final Set<ExecutionAttemptID> pendingDeployments = new HashSet<>();
    private final Map<ResourceID, Set<ExecutionAttemptID>> executionsByHost = new HashMap<>();
    private final Map<ExecutionAttemptID, ResourceID> hostByExecution = new HashMap<>();

    @Override
    public void startTrackingPendingDeploymentOf(
            ExecutionAttemptID executionAttemptId, ResourceID host) {
        pendingDeployments.add(executionAttemptId);
        hostByExecution.put(executionAttemptId, host);
        executionsByHost.computeIfAbsent(host, ignored -> new HashSet<>()).add(executionAttemptId);
    }

    @Override
    public void completeDeploymentOf(ExecutionAttemptID executionAttemptId) {
        pendingDeployments.remove(executionAttemptId);
    }

    @Override
    public void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId) {
        pendingDeployments.remove(executionAttemptId);
        ResourceID host = hostByExecution.remove(executionAttemptId);
        if (host != null) {
            executionsByHost.computeIfPresent(
                    host,
                    (resourceID, executionAttemptIds) -> {
                        executionAttemptIds.remove(executionAttemptId);

                        return executionAttemptIds.isEmpty() ? null : executionAttemptIds;
                    });
        }
    }

    @Override
    public Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host) {
        return executionsByHost.getOrDefault(host, Collections.emptySet()).stream()
                .collect(
                        Collectors.toMap(
                                x -> x,
                                x ->
                                        pendingDeployments.contains(x)
                                                ? ExecutionDeploymentState.PENDING
                                                : ExecutionDeploymentState.DEPLOYED));
    }
}
