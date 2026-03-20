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

/**
 * A watcher on {@link ExecutionPlanStore}. It could monitor all the changes on the execution plan
 * store and notify the {@link ExecutionPlanStore} via {@link
 * ExecutionPlanStore.ExecutionPlanListener}.
 *
 * <p><strong>Important</strong>: The {@link ExecutionPlanStoreWatcher} could not guarantee that
 * there is no {@link ExecutionPlanStore.ExecutionPlanListener} callbacks happen after {@link
 * #stop()}. So the implementor is responsible for filtering out these spurious callbacks.
 *
 * <p>【学习型注释】
 * ExecutionPlanStoreWatcher 用于监视执行计划存储的变化。
 * 在高可用模式下，当其他 JobManager 实例添加或删除执行计划时，
 * 当前实例需要通过 Watcher 机制感知这些变化，以便进行作业恢复或状态同步。
 * 实现类包括 ZooKeeperExecutionPlanStoreWatcher（基于 ZK 监听）等。
 */
public interface ExecutionPlanStoreWatcher {

    /**
     * Start the watcher on {@link ExecutionPlanStore}.
     *
     * @param executionPlanListener use executionPlanListener to notify the {@link
     *     DefaultExecutionPlanStore}
     * @throws Exception when start internal services
     */
    void start(ExecutionPlanStore.ExecutionPlanListener executionPlanListener) throws Exception;

    /**
     * Stop the watcher on {@link ExecutionPlanStore}.
     *
     * @throws Exception when stop internal services
     */
    void stop() throws Exception;
}
