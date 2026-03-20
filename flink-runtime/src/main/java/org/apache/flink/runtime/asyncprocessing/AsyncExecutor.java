// 这个文件已经全部加上中文注释
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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/**
 * Executor for executing batch {@link AsyncRequest}s.
 *
 * <p>Notice that the owner who create the {@code AsyncExecutor} is responsible for shutting down it
 * when it is no longer in use.
 */
// 【学习型注释】异步请求执行器接口，负责批量执行 AsyncRequest。
// 核心设计思想：
// 1. 批量处理：将零散的异步状态请求收集到 AsyncRequestContainer 中进行批量执行，极大提高底层 I/O 效率。
// 2. 流量控制：fullyLoaded() 方法用于向 AEC 反馈当前的负载情况，避免提交过多请求导致内存溢出。
// 3. 阻塞执行支持：executeRequestSync 提供同步 API 的兼容支持，允许在异步框架内执行同步逻辑。
@Internal
public interface AsyncExecutor<REQUEST extends AsyncRequest<?>> {
    /**
     * Execute a batch of async requests.
     *
     * @param asyncRequestContainer The AsyncRequestContainer which holds the given batch of
     *     processing requests.
     * @return A future can determine whether execution has completed.
     */
    CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<REQUEST> asyncRequestContainer);

    /**
     * Create a {@link AsyncRequestContainer} which is used to hold the batched {@link
     * AsyncRequest}.
     */
    AsyncRequestContainer<REQUEST> createRequestContainer();

    /**
     * Execute a single async request *synchronously*. This is for synchronous APIs.
     *
     * @param asyncRequest the request to run.
     */
    void executeRequestSync(REQUEST asyncRequest);

    /**
     * Check if this executor is fully loaded. Will be invoked to determine whether to give more
     * requests to run or wait for a while.
     *
     * @return the count.
     */
    boolean fullyLoaded();

    /** Shutdown the StateExecutor, and new committed state execution requests will be rejected. */
    void shutdown();
}
