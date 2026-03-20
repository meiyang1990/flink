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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.streaming.api.graph.ExecutionPlan;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link ExecutionPlan} instances for JobManagers running in {@link HighAvailabilityMode#NONE}.
 *
 * <p>All operations are NoOps, because {@link ExecutionPlan} instances cannot be recovered in this
 * recovery mode.
 *
 * <p>【学习型注释】
 * StandaloneExecutionPlanStore 是单机模式（无高可用）下的空实现。
 * 由于单机模式下 JobManager 故障后无法恢复，因此所有持久化操作都是空操作（NoOp）。
 * 这减少了单机部署时的不必要的存储开销。
 */
public class StandaloneExecutionPlanStore implements ExecutionPlanStore {

    @Override
    public void start(ExecutionPlanListener executionPlanListener) throws Exception {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to do
    }

    @Override
    public void putExecutionPlan(ExecutionPlan jobGraph) {
        // Nothing to do
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) {
        // Nothing to do
    }

    @Override
    public Collection<JobID> getJobIds() {
        return Collections.emptyList();
    }

    @Override
    public ExecutionPlan recoverExecutionPlan(JobID jobId) {
        return null;
    }
}
