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

import org.apache.flink.runtime.highavailability.JobResultStore;

/**
 * Factory for components that are responsible for persisting a job for recovery.
 *
 * <p>【学习型注释】
 * JobPersistenceComponentFactory 是作业持久化组件的工厂接口。
 * 在高可用模式下，Flink 需要将作业的执行计划和结果持久化，以便 JobManager 故障后能够恢复。
 * 该工厂负责创建两类核心组件：
 * - ExecutionPlanStore: 存储作业执行计划
 * - JobResultStore: 存储作业执行结果
 */
public interface JobPersistenceComponentFactory {

    /**
     * Creates a {@link ExecutionPlanStore}.
     *
     * @return a {@code ExecutionPlanStore} instance
     */
    ExecutionPlanStore createExecutionPlanStore();

    /**
     * Creates {@link JobResultStore} instances.
     *
     * @return a {@code JobResultStore} instance.
     */
    JobResultStore createJobResultStore();
}
