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

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

/**
 * {@link JobPersistenceComponentFactory} implementation which creates a {@link ExecutionPlanStore}
 * using the provided {@link HighAvailabilityServices}.
 *
 * <p>【学习型注释】
 * HaServicesJobPersistenceComponentFactory 是基于高可用服务的作业持久化组件工厂实现。
 * 它使用 HighAvailabilityServices 来创建 ExecutionPlanStore 和 JobResultStore，
 * 确保在高可用模式下作业状态和结果能够正确持久化到分布式存储（如 ZooKeeper）。
 */
public class HaServicesJobPersistenceComponentFactory implements JobPersistenceComponentFactory {
    private final HighAvailabilityServices highAvailabilityServices;

    public HaServicesJobPersistenceComponentFactory(
            HighAvailabilityServices highAvailabilityServices) {
        this.highAvailabilityServices = highAvailabilityServices;
    }

    @Override
    public ExecutionPlanStore createExecutionPlanStore() {
        return create(highAvailabilityServices::getExecutionPlanStore, ExecutionPlanStore.class);
    }

    @Override
    public JobResultStore createJobResultStore() {
        return create(highAvailabilityServices::getJobResultStore, JobResultStore.class);
    }

    private <T> T create(SupplierWithException<T, ? extends Exception> supplier, Class<T> clazz) {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not create %s from %s.",
                            clazz.getSimpleName(),
                            highAvailabilityServices.getClass().getSimpleName()),
                    e);
        }
    }
}
