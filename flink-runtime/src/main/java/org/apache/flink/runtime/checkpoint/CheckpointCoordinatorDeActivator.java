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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.JobStatusListener;

/**
 * This actor listens to changes in the JobStatus and activates or deactivates the periodic
 * checkpoint scheduler.
 * 
 * <p>【学习型注释】中文解释：监听作业状态（JobStatus）的变更，自动开启或关闭周期性检查点调度器，是协调器生命周期管理的一部分。
 */
public interface CheckpointCoordinatorDeActivator
        extends JobStatusListener, ExecutionStateUpdateListener {

    /** 静态工厂方法，用于创建总是停止检查点调度器的 Deactivator */
    static CheckpointCoordinatorDeActivator alwaysStopping(CheckpointCoordinator coordinator) {
        return (jobId, newJobStatus, timestamp) -> coordinator.stopCheckpointScheduler();
    }

    /** 监听执行状态变更，根据需要进行逻辑扩展，当前默认实现为空 */
    @Override
    default void onStateUpdate(
            ExecutionAttemptID execution, ExecutionState previousState, ExecutionState newState) {}
}
