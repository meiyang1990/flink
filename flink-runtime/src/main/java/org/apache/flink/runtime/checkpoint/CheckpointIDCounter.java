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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;

import java.util.concurrent.CompletableFuture;

/** 
 * A checkpoint ID counter. 
 * 
 * <p>【学习型注释】中文解释：检查点 ID 生成器接口。确保在分布式环境下，即便 JobMaster 发生故障重启，
 * 也能为每个检查点生成全局唯一的、单调递增的 ID。
 */
public interface CheckpointIDCounter {
    /** 初始检查点 ID，从 1 开始 */
    int INITIAL_CHECKPOINT_ID = 1;

    /** 启动 ID 计数器服务 */
    void start() throws Exception;

    /**
     * Shuts the {@link CheckpointIDCounter} service.
     *
     * <p>The job status is forwarded and used to decide whether state should actually be discarded
     * or kept.
     *
     * @param jobStatus Job state on shut down
     * @return The {@code CompletableFuture} holding the result of the shutdown operation.
     */
    CompletableFuture<Void> shutdown(JobStatus jobStatus);

    /**
     * Atomically increments the current checkpoint ID.
     *
     * @return The previous checkpoint ID
     */
    long getAndIncrement() throws Exception;

    /**
     * Atomically gets the current checkpoint ID.
     *
     * @return The current checkpoint ID
     */
    long get();

    /**
     * Sets the current checkpoint ID.
     *
     * @param newId The new ID
     */
    void setCount(long newId) throws Exception;
}
