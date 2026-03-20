// 这个文件已经全部加上中文注释
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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

/** 
 * RPC Gateway interface for messages to the CheckpointCoordinator. 
 * 
 * <p>【学习型注释】中文解释：用于 TaskManager 向 JobManager 中的 CheckpointCoordinator 发送检查点相关 RPC 消息的网关接口。
 * 包括 ack、decline 以及指标上报。
 */
public interface CheckpointCoordinatorGateway extends RpcGateway {

    /** 任务执行确认：子任务完成检查点快照后调用此方法向协调器确认 */
    void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> subtaskState);

    /** 任务拒绝检查点：子任务拒绝当前检查点时调用 */
    void declineCheckpoint(DeclineCheckpoint declineCheckpoint);

    /** 上报检查点指标信息 */
    void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics);

    /** 上报初始化指标信息 */
    void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics);
}
