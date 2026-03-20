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
 * Singleton {@link ExecutionPlanStoreWatcher} empty implementation.
 *
 * <p>【学习型注释】
 * NoOpExecutionPlanStoreWatcher 是 ExecutionPlanStoreWatcher 的空实现（单例模式）。
 * 在单机模式（无高可用）下使用，因为不需要监视外部存储的变化，
 * 所有方法都是空操作，减少不必要的资源开销。
 */
public enum NoOpExecutionPlanStoreWatcher implements ExecutionPlanStoreWatcher {
    INSTANCE;

    @Override
    public void start(ExecutionPlanStore.ExecutionPlanListener executionPlanListener) {
        // noop
    }

    @Override
    public void stop() {
        // noop
    }
}
