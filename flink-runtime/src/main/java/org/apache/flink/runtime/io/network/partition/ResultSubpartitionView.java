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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A view to consume a {@link ResultSubpartition} instance.
 *
 * <p>【学习型注释】ResultSubpartitionView 是 ResultSubpartition 的消费者视图接口。
 *
 * <h2>设计目的</h2>
 * <ul>
 *   <li>将生产者（ResultSubpartition）和消费者（InputChannel）解耦</li>
 *   <li>提供统一的数据读取接口，屏蔽底层存储差异</li>
 *   <li>支持流控和背压（通过 backlog 和 availability 机制）</li>
 * </ul>
 *
 * <h2>实现类</h2>
 * <ul>
 *   <li>PipelinedSubpartitionView：流水线模式，内存中的数据视图</li>
 *   <li>BoundedBlockingSubpartitionReader：批处理模式，可能涉及磁盘读取</li>
 *   <li>各种 Shuffle 模式的专用实现</li>
 * </ul>
 *
 * <h2>数据流向</h2>
 * <pre>
 * ResultSubpartition → ResultSubpartitionView → PartitionRequestQueue → Netty → RemoteInputChannel
 *                  (生产者)        (消费视图)      (服务端队列)   (网络传输)    (下游消费)
 * </pre>
 *
 * <h2>关键方法</h2>
 * <ul>
 *   <li>{@link #getNextBuffer()}：获取下一个缓冲区（非阻塞）</li>
 *   <li>{@link #notifyDataAvailable()}：通知有新数据可读</li>
 *   <li>{@link #getAvailabilityAndBacklog(boolean)}：查询可用性和积压量</li>
 *   <li>{@link #resumeConsumption()}：恢复消费（配合 Checkpoint 使用）</li>
 * </ul>
 */
public interface ResultSubpartitionView {

    /**
     * Returns the next {@link Buffer} instance of this queue iterator.
     *
     * <p>If there is currently no instance available, it will return <code>null</code>. This might
     * happen for example when a pipelined queue producer is slower than the consumer or a spilled
     * queue needs to read in more data.
     *
     * <p><strong>Important</strong>: The consumer has to make sure that each buffer instance will
     * eventually be recycled with {@link Buffer#recycleBuffer()} after it has been consumed.
     *
     * <p>【学习型注释】获取下一个缓冲区（非阻塞）。
     * <ul>
     *   <li>返回 null 表示当前无数据可读（生产者较慢或数据正在从磁盘加载）</li>
     *   <li>调用方必须确保使用完后调用 recycleBuffer() 归还缓冲区</li>
     *   <li>返回的 BufferAndBacklog 同时包含 Buffer 和当前积压量信息</li>
     * </ul>
     */
    @Nullable
    BufferAndBacklog getNextBuffer() throws IOException;

    /**
     * 通知有新数据可读。当 ResultSubpartition 有新数据写入时调用此方法，
     * 触发消费端（PartitionRequestQueue）检查并发送数据。
     */
    void notifyDataAvailable();

    /**
     * 通知有高优先级事件（如 Checkpoint Barrier）到达。
     * @param priorityBufferNumber 优先级缓冲区在队列中的位置
     */
    default void notifyPriorityEvent(int priorityBufferNumber) {}

    /**
     * 释放视图持有的所有资源。在 InputChannel 关闭或出错时调用。
     */
    void releaseAllResources() throws IOException;

    /**
     * 检查视图是否已释放。
     */
    boolean isReleased();

    /**
     * 恢复消费。用于 Checkpoint 后恢复数据读取，配合 Aligned Checkpoint 的对齐机制。
     */
    void resumeConsumption();

    /**
     * 确认所有用户数据已处理完成。用于增量 Checkpoint 的完成确认。
     */
    void acknowledgeAllDataProcessed();

    /**
     * {@link ResultSubpartitionView} can decide whether the failure cause should be reported to
     * consumer as failure (primary failure) or {@link ProducerFailedException} (secondary failure).
     * Secondary failure can be reported only if producer (upstream task) is guaranteed to failover.
     *
     * <p><strong>BEWARE:</strong> Incorrectly reporting failure cause as primary failure, can hide
     * the root cause of the failure from the user.
     */
    Throwable getFailureCause();

    /**
     * Get the availability and backlog of the view. The availability represents if the view is
     * ready to get buffer from it. The backlog represents the number of available data buffers.
     *
     * @param isCreditAvailable the availability of credits for this {@link ResultSubpartitionView}.
     * @return availability and backlog.
     */
    AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable);

    int unsynchronizedGetNumberOfQueuedBuffers();

    int getNumberOfQueuedBuffers();

    void notifyNewBufferSize(int newBufferSize);

    /**
     * In tiered storage shuffle mode, only required segments will be sent to prevent the redundant
     * buffer usage. Downstream will notify the upstream by this method to send required segments.
     *
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    default void notifyRequiredSegmentId(int subpartitionId, int segmentId) {}

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    int peekNextBufferSubpartitionId() throws IOException;

    /**
     * Availability of the {@link ResultSubpartitionView} and the backlog in the corresponding
     * {@link ResultSubpartition}.
     *
     * <p>【学习型注释】可用性与积压量的组合信息，用于 Credit-based 流量控制。
     * <ul>
     *   <li>isAvailable：是否有数据可读，结合消费者的 Credit 判断是否应该发送</li>
     *   <li>backlog：当前积压的数据缓冲区数量，消费者据此分配 Credit</li>
     * </ul>
     *
     * <p>在 Credit-based 流控中：
     * <ol>
     *   <li>生产者通过 backlog 告知消费者还有多少数据待发送</li>
     *   <li>消费者根据 backlog 和本地缓冲区情况分配 Credit</li>
     *   <li>生产者只有在消费者有足够 Credit 时才发送数据</li>
     * </ol>
     */
    class AvailabilityWithBacklog {

        /** 是否有数据可读取 */
        private final boolean isAvailable;

        /** 当前积压的缓冲区数量（未发送的数据量） */
        private final int backlog;

        public AvailabilityWithBacklog(boolean isAvailable, int backlog) {
            checkArgument(backlog >= 0, "Backlog must be non-negative.");

            this.isAvailable = isAvailable;
            this.backlog = backlog;
        }

        public boolean isAvailable() {
            return isAvailable;
        }

        public int getBacklog() {
            return backlog;
        }
    }
}
