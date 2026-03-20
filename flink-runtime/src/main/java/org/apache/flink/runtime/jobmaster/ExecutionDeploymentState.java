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

/**
 * Possible states for the deployment of an execution.
 *
 * <p>【学习型注释】
 * ExecutionDeploymentState 定义了 Execution（Task 执行尝试）的部署状态：
 * - PENDING: 部署请求已发送或即将发送，等待 TaskExecutor 确认
 * - DEPLOYED: TaskExecutor 已确认部署，Task 正在运行
 * 该状态用于跟踪 Task 从提交到实际运行的过程，是部署一致性检查的重要依据。
 */
public enum ExecutionDeploymentState {
    /** The deployment has or is about to be started. */
    PENDING,
    /** The deployment has been acknowledged by the TaskExecutor. */
    DEPLOYED
}
