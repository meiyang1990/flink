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

import java.util.Map;

/**
 * A tracker for deployed executions.
 *
 * <p>【学习型注释】
 * ExecutionDeploymentTracker 用于跟踪 Task 的部署状态。
 * 当 JobMaster 向 TaskExecutor 提交 Task 时，需要记录部署请求的状态：
 * - 已发送部署请求但尚未收到确认（Pending）
 * - 部署完成并收到 TaskExecutor 确认（Complete）
 * - 部署失败或 Task 被取消（Stop Tracking）
 * 该跟踪器支持部署一致性检查，用于发现和修复 JobMaster 与 TaskExecutor 之间的状态不一致。
 */
public interface ExecutionDeploymentTracker {

    /**
     * Starts tracking the given execution that is being deployed on the given host.
     *
     * @param executionAttemptId execution to start tracking
     * @param host hosting task executor
     */
    void startTrackingPendingDeploymentOf(ExecutionAttemptID executionAttemptId, ResourceID host);

    /**
     * Marks the deployment of the given execution as complete.
     *
     * @param executionAttemptId execution whose deployment to mark as complete
     */
    void completeDeploymentOf(ExecutionAttemptID executionAttemptId);

    /**
     * Stops tracking the given execution.
     *
     * @param executionAttemptId execution to stop tracking
     */
    void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId);

    /**
     * Returns all tracked executions for the given host.
     *
     * @param host hosting task executor
     * @return tracked executions
     */
    Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host);
}
