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

import org.apache.flink.util.concurrent.ScheduledExecutor;

/**
 * Provides the context for {@link DefaultCheckpointPlanCalculator} to compute the plan of
 * checkpoints.
 *
 * <p>【学习型注释】中文解释：检查点计划计算器上下文接口。
 * 为 {@link DefaultCheckpointPlanCalculator} 提供必要的外部运行环境，
 * 例如主线程执行器（用于调度任务）以及任务完成状态检查，以辅助完成检查点计划的计算。
 */
public interface CheckpointPlanCalculatorContext {

    /**
     * Acquires the main thread executor for this job.
     *
     * @return The main thread executor.
     */
    ScheduledExecutor getMainExecutor();

    /**
     * Detects whether there are already some tasks finished.
     *
     * @return Whether there are finished tasks.
     */
    boolean hasFinishedTasks();
}
