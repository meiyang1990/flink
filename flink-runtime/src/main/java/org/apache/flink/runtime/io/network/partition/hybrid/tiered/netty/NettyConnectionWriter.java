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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import javax.annotation.Nullable;

/**
 * 【中文说明】NettyConnectionWriter 是 Producer 端代理用于将缓冲区写入 Netty 连接的接口。
 *
 * <p>核心职责：
 * <ul>
 *   <li>维护待发送数据的内部队列。</li>
 *   <li>Netty 服务端将从该队列中获取缓冲区并将其推送给 Consumer 端。</li>
 * </ul>
 */
public interface NettyConnectionWriter {
    /**
     * 【中文说明】向 Netty 连接写入 payload 数据（如缓冲区或 Segment 标识）。
     *
     * @param nettyPayload 要发送的 Netty 负载数据
     */
    void writeNettyPayload(NettyPayload nettyPayload);

    /**
     * 【中文说明】获取此 Writer 关联的连接唯一标识 ID。
     *
     * @return 连接 ID
     */
    NettyConnectionId getNettyConnectionId();

    /** 【中文说明】通知 Writer 中已有新的缓冲区可用，可触发推送逻辑。 */
    void notifyAvailable();

    /**
     * 【中文说明】获取当前已写入但尚未发送的 Payload 总数（包含 Buffer 和其他控制消息）。
     *
     * @return 队列中待发送的 Payload 总数
     */
    int numQueuedPayloads();

    /**
     * 【中文说明】获取当前已写入但尚未发送的缓冲区 Buffer Payload 数量。
     *
     * @return 待发送的缓冲区数量
     */
    int numQueuedBufferPayloads();

    /**
     * 【中文说明】关闭 Writer。
     *
     * <p>若 error 为 null，则清空并回收所有待发送缓冲区。
     * <p>若 error 不为 null，则在清空缓冲区后发送错误信息，通知 Consumer 发生了异常。
     *
     * @param error 若存在异常，则此参数为异常信息，否则为 null
     */
    void close(@Nullable Throwable error);
}
