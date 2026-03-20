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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/**
 * Interface which specifies the JobMaster service.
 *
 * <p>【学习型注释】
 * JobMasterService 定义了 JobMaster 服务的核心接口。
 * 它是 JobMaster 运行时的抽象表示，提供以下功能：
 * - getGateway(): 获取 JobMasterGateway，用于接收外部 RPC 调用
 * - getAddress(): 获取服务地址，其他组件通过该地址连接到 JobMaster
 * - getTerminationFuture(): 获取终止 Future，用于监听服务终止状态
 * 实现类（如 JobMaster）通过该接口对外暴露服务能力，同时隐藏内部实现细节。
 */
public interface JobMasterService extends AutoCloseableAsync {

    /**
     * Get the {@link JobMasterGateway} belonging to this service.
     *
     * @return JobMasterGateway belonging to this service
     */
    JobMasterGateway getGateway();

    /**
     * Get the address of the JobMaster service under which it is reachable.
     *
     * @return Address of the JobMaster service
     */
    String getAddress();

    /**
     * Get the termination future of this job master service.
     *
     * @return future which is completed once the JobMasterService completes termination
     */
    CompletableFuture<Void> getTerminationFuture();
}
