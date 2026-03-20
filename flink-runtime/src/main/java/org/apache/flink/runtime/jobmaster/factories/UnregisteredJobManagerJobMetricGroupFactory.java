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

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.api.graph.ExecutionPlan;

import javax.annotation.Nonnull;

/**
 * {@link JobManagerJobMetricGroupFactory} which returns an unregistered {@link
 * JobManagerJobMetricGroup}.
 *
 * <p>【学习型注释】UnregisteredJobManagerJobMetricGroupFactory 是一个单例工厂，返回未注册的指标组。
 * 使用场景：当不需要实际收集指标时（如测试环境、指标功能被禁用时），使用此工厂避免不必要的性能开销。
 * 采用枚举单例模式（INSTANCE），确保全局唯一且线程安全。
 */
public enum UnregisteredJobManagerJobMetricGroupFactory implements JobManagerJobMetricGroupFactory {
    INSTANCE;

    @Override
    public JobManagerJobMetricGroup create(@Nonnull ExecutionPlan executionPlan) {
        return UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
    }
}
