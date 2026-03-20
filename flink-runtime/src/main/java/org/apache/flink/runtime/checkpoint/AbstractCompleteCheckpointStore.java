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
import org.apache.flink.runtime.state.SharedStateRegistry;

import java.util.Deque;
import java.util.Optional;

/**
 * The abstract class of {@link CompletedCheckpointStore}, which holds the {@link
 * SharedStateRegistry} and provides the registration of shared state.
 * 
 * <p>【学习型注释】中文解释：{@link CompletedCheckpointStore} 的抽象基类，主要负责持有 {@link SharedStateRegistry} 
 * 并提供共享状态的注册与管理功能，用于实现跨检查点的状态共享。
 */
public abstract class AbstractCompleteCheckpointStore implements CompletedCheckpointStore {
    private final SharedStateRegistry sharedStateRegistry;

    public AbstractCompleteCheckpointStore(SharedStateRegistry sharedStateRegistry) {
        this.sharedStateRegistry = sharedStateRegistry;
    }

    @Override
    public SharedStateRegistry getSharedStateRegistry() {
        return sharedStateRegistry;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        // 如果作业处于全局终止状态，则关闭共享状态注册表，清理相关资源
        if (jobStatus.isGloballyTerminalState()) {
            sharedStateRegistry.close();
        }
    }

    /**
     * Unregister shared states that are no longer in use. Should be called after completing a
     * checkpoint (even if no checkpoint was subsumed, so that state added by an aborted checkpoints
     * and not used later can be removed).
     * 
     * <p>【学习型注释】中文解释：注销不再使用的共享状态。在完成检查点后调用，即使没有检查点被合并（subsumed），
     * 也可以清除那些被异常检查点添加且后续未被使用的状态。
     */
    protected void unregisterUnusedState(Deque<CompletedCheckpoint> unSubsumedCheckpoints) {
        // 查找最低检查点 ID 并据此清理共享状态
        findLowest(unSubsumedCheckpoints).ifPresent(sharedStateRegistry::unregisterUnusedState);
    }

    protected static Optional<Long> findLowest(Deque<CompletedCheckpoint> unSubsumedCheckpoints) {
        for (CompletedCheckpoint p : unSubsumedCheckpoints) {
            // 如果检查点不是保存点，则将其作为候选最低点
            if (!p.getProperties().isSavepoint()) {
                return Optional.of(p.getCheckpointID());
            }
        }
        return Optional.empty();
    }
}
