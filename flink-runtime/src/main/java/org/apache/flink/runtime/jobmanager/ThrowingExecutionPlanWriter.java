/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.streaming.api.graph.ExecutionPlan;

/**
 * {@link ExecutionPlanWriter} implementation which does not allow to store {@link ExecutionPlan}.
 *
 * <p>【学习型注释】
 * ThrowingExecutionPlanWriter 是一个特殊的 ExecutionPlanWriter 实现（单例模式），
 * 它禁止任何写入操作，所有方法都会抛出 UnsupportedOperationException。
 * 用于在特定场景下（如只读模式或测试环境）明确禁止执行计划的持久化操作。
 */
public enum ThrowingExecutionPlanWriter implements ExecutionPlanWriter {
    INSTANCE;

    @Override
    public void putExecutionPlan(ExecutionPlan jobGraph) {
        throw new UnsupportedOperationException("Cannot store job graphs.");
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) {
        throw new UnsupportedOperationException("Cannot persist job resource requirements.");
    }
}
