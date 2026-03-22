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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

/**
 * The producer-side agent of a Tier.
 *
 * <p>Note that when writing a buffer to a tier, the {@link TierProducerAgent} should first call
 * {@code tryStartNewSegment} to start a new segment. The agent can then continue writing the buffer
 * to the tier as long as the return value of {@code write} is true. If the return value of {@code
 * write} is false, it indicates that the current segment can no longer store the buffer, and the
 * agent should try to start a new segment before writing the buffer.
 */
public interface TierProducerAgent extends AutoCloseable {

    /**
     * Try to start a new segment in the Tier.
     *
     * @param subpartitionId subpartition id that the new segment belongs to
     * @param segmentId id of the new segment
     * @param minNumBuffers the minimum number of buffers that are expected to be written to the
     *     newly created segment.
     * @return true if the segment can be started, false otherwise.
     */
    boolean tryStartNewSegment(
            TieredStorageSubpartitionId subpartitionId, int segmentId, int minNumBuffers);

    /**
     * Writes the finished {@link Buffer} to the consumer.
     *
     * <p>Note that the method is successfully executed (without throwing any exception), the buffer
     * should be released by the caller, otherwise the tier should be responsible to recycle the
     * buffer.
     *
     * @param subpartitionId the subpartition id that the buffer is writing to
     * @param finishedBuffer the writing buffer
     * @param bufferOwner the current owner of this writing buffer
     * @param numRemainingConsecutiveBuffers number of buffers that would be passed in the following
     *     invocations and should be written to the same segment as this one
     * @return return true if the buffer is written successfully, return false if the current
     *     segment can not store this buffer(and the following consecutive ones) and the current
     *     segment is finished. When returning false, the agent should try start a new segment
     *     before writing the buffer.
     */
    boolean tryWrite(
            TieredStorageSubpartitionId subpartitionId,
            Buffer finishedBuffer,
            Object bufferOwner,
            int numRemainingConsecutiveBuffers);

    /**
     * Close the agent.
     *
     * <p>Note this only releases resources directly hold by the agent, which excludes resources
     * managed by the resource registry.
     *
     * <p>【中文说明】关闭代理并释放资源。
     *
     * <p>注意：此方法仅释放代理直接持有的资源（如内部缓存），
     * 不包括通过 {@link org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry}
     * 注册的资源，那些资源由 Registry 统一管理释放。
     */
    void close();
}
