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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Main accumulator registry which encapsulates user-defined accumulators. */
// 【学习型注释】累加器注册表，运行在 TaskManager 端，管理当前任务执行过程中所有用户自定义的累加器。
// 当任务执行完毕时，通过 getSnapshot() 创建快照并发送给 JobManager 进行聚合汇总。
public class AccumulatorRegistry {

    protected static final Logger LOG = LoggerFactory.getLogger(AccumulatorRegistry.class);

    protected final JobID jobID;
    protected final ExecutionAttemptID taskID;

    /* User-defined Accumulator values stored for the executing task. */
    private final Map<String, Accumulator<?, ?>> userAccumulators = new ConcurrentHashMap<>(4);

    public AccumulatorRegistry(JobID jobID, ExecutionAttemptID taskID) {
        this.jobID = jobID;
        this.taskID = taskID;
    }

    /**
     * Creates a snapshot of this accumulator registry.
     *
     * @return a serialized accumulator map
     */
    public AccumulatorSnapshot getSnapshot() {
        try {
            return new AccumulatorSnapshot(jobID, taskID, userAccumulators);
        } catch (Throwable e) {
            LOG.warn("Failed to serialize accumulators for task.", e);
            return null;
        }
    }

    /** Gets the map for user-defined accumulators. */
    public Map<String, Accumulator<?, ?>> getUserMap() {
        return userAccumulators;
    }
}
