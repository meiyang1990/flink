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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

/**
 * This interface is used by {@link HashSubpartitionBufferAccumulator} to operate {@link
 * HashBufferAccumulator}.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>HashSubpartitionBufferAccumulatorContext 是子分区缓冲区累积器与父级 HashBufferAccumulator
 * 之间的上下文接口。通过此接口，子分区累积器可以：
 *
 * <ul>
 *   <li>从全局缓冲池申请新的缓冲区</li>
 *   <li>将累积完成的缓冲区刷新到下游 Tier 层</li>
 * </ul>
 *
 * <h2>调用关系</h2>
 *
 * <pre>
 *   HashBufferAccumulator (实现此接口)
 *           ▲
 *           │ provides context
 *           │
 *   HashSubpartitionBufferAccumulator (使用此接口)
 *           │
 *           │ requestBufferBlocking()
 *           │ flushAccumulatedBuffers()
 *           ▼
 *   TieredStorageMemoryManager / TierProducerAgent
 * </pre>
 */
public interface HashSubpartitionBufferAccumulatorContext {

    /**
     * Request {@link BufferBuilder} from the {@link BufferPool}.
     *
     * <p>从缓冲池申请一个 BufferBuilder（阻塞操作）。
     * 由 HashBufferAccumulator 委托给 TieredStorageMemoryManager 执行。
     *
     * @return the requested buffer 申请到的缓冲区构建器
     */
    BufferBuilder requestBufferBlocking();

    /**
     * Flush the accumulated {@link Buffer}s of the subpartition.
     *
     * <p>将子分区累积的缓冲区刷新到下游 Tier 层。
     *
     * @param subpartitionId the subpartition id 子分区标识符
     * @param accumulatedBuffer the accumulated buffer 累积完成的缓冲区
     * @param numRemainingConsecutiveBuffers number of buffers that would be passed in the following
     *     invocations and should be written to the same segment as this one
     *     后续将传入的缓冲区数量，这些缓冲区应写入同一个 Segment
     */
    void flushAccumulatedBuffers(
            TieredStorageSubpartitionId subpartitionId,
            Buffer accumulatedBuffer,
            int numRemainingConsecutiveBuffers);
}
