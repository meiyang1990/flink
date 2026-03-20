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
import org.apache.flink.runtime.util.ZooKeeperUtils;

/**
 * Singleton {@link ExecutionPlanStoreUtil} implementation for ZooKeeper.
 *
 * <p>【学习型注释】
 * ZooKeeperExecutionPlanStoreUtil 是 ExecutionPlanStoreUtil 的 ZooKeeper 实现（单例模式）。
 * 它提供了 JobID 与 ZooKeeper 路径之间的转换：
 * - jobIDToName: 将 JobID 转换为 ZK 路径（如 /flink/job-xxx）
 * - nameToJobID: 从 ZK 路径解析出 JobID
 * 用于在高可用模式下将执行计划存储到 ZooKeeper 的特定路径结构中。
 */
public enum ZooKeeperExecutionPlanStoreUtil implements ExecutionPlanStoreUtil {
    INSTANCE;

    @Override
    public String jobIDToName(JobID jobId) {
        return ZooKeeperUtils.getPathForJob(jobId);
    }

    @Override
    public JobID nameToJobID(String name) {
        return JobID.fromHexString(name);
    }
}
