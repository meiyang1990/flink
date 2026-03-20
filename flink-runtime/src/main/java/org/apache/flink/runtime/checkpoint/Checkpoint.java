// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** 
 * A checkpoint, pending or completed. 
 * 
 * <p>【学习型注释】中文解释：检查点的通用接口，无论是正在进行中（Pending）的还是已完成（Completed）的检查点，
 * 都需要实现此接口，用于管理检查点的标识和垃圾回收（Discard）。
 */
public interface Checkpoint {
    DiscardObject NOOP_DISCARD_OBJECT = () -> {};

    /** 获取检查点的唯一标识 ID */
    long getCheckpointID();

    /**
     * This method precede the {@link DiscardObject#discard()} method and should be called from the
     * {@link CheckpointCoordinator}(under the lock) while {@link DiscardObject#discard()} can be
     * called from any thread/place.
     * 
     * <p>【学习型注释】中文解释：将检查点标记为已丢弃。此操作应在 {@link CheckpointCoordinator} 的锁控制下执行，
     * 用于确保原子性。标记后，检查点对应的资源可以异步回收。
     */
    DiscardObject markAsDiscarded();

    /** Extra interface for discarding the checkpoint. */
    interface DiscardObject {
        /** 执行检查点资源的删除/丢弃操作 */
        void discard() throws Exception;

        /** 异步执行丢弃操作，通过指定的 IO 执行器完成，避免阻塞主流程 */
        default CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            return FutureUtils.runAsync(this::discard, ioExecutor);
        }
    }
}
