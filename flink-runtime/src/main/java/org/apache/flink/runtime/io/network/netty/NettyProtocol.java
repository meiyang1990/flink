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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

/**
 * Netty 网络协议定义类 - 定义服务端和客户端的 ChannelHandler 管道配置。
 *
 * <p>核心职责：
 * <ul>
 *   <li>定义服务端 Channel Pipeline：消息解码 -> 请求处理 -> 数据出队发送</li>
 *   <li>定义客户端 Channel Pipeline：消息编码 -> 响应解码 -> 响应处理</li>
 *   <li>封装 Netty Handler 的创建和组装逻辑</li>
 * </ul>
 *
 * <p>服务端数据流向（上游为生产者 TaskManager）：
 * <pre>
 *   网络接收 -> NettyMessageDecoder(解码) -> PartitionRequestServerHandler(处理请求)
 *           -> PartitionRequestQueue(数据出队) -> NettyMessageEncoder(编码) -> 网络发送
 * </pre>
 *
 * <p>客户端数据流向（下游为消费者 TaskManager）：
 * <pre>
 *   请求发送 -> NettyMessageEncoder(编码) -> 网络
 *   网络接收 -> NettyMessageClientDecoderDelegate(解码) -> CreditBasedPartitionRequestClientHandler(处理响应)
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>NettyServer 启动时调用 getServerChannelHandlers() 配置服务端管道</li>
 *   <li>NettyClient 连接时调用 getClientChannelHandlers() 配置客户端管道</li>
 * </ul>
 *
 * <p>与其他组件的关系：
 * <ul>
 *   <li>NettyConnectionManager：持有本类实例，用于初始化 Client 和 Server</li>
 *   <li>ResultPartitionProvider：服务端通过它获取要发送的数据分区</li>
 *   <li>TaskEventPublisher：处理反向传播的 Task 事件（如 Barrier 事件）</li>
 * </ul>
 *
 * Defines the server and client channel handlers, i.e. the protocol, used by netty.
 */
public class NettyProtocol {

    /**
     * 消息编码器，可共享（@Sharable），用于将 NettyMessage 序列化到网络。
     * 服务端和客户端复用同一个编码器实例，因为编码逻辑是无状态的。
     */
    private final NettyMessage.NettyMessageEncoder messageEncoder =
            new NettyMessage.NettyMessageEncoder();

    /**
     * 结果分区提供者，服务端通过它查找并获取 ResultSubpartition 的数据。
     * 当收到 PartitionRequest 时，用 partitionId 从 provider 获取对应的 ResultSubpartitionView。
     */
    private final ResultPartitionProvider partitionProvider;

    /**
     * Task 事件发布者，用于处理 TaskEventRequest（如 Checkpoint Barrier 的反向确认）。
     * 在流水线执行中，下游 Task 可通过此机制向上游 Task 发送事件。
     */
    private final TaskEventPublisher taskEventPublisher;

    /**
     * 构造 NettyProtocol 实例。
     *
     * @param partitionProvider 结果分区提供者，用于服务端获取数据
     * @param taskEventPublisher Task 事件发布者，用于处理反向事件
     */
    NettyProtocol(
            ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher) {
        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
    }

    /**
     * Returns the server channel handlers.
     *
     * <pre>
     * +-------------------------------------------------------------------+
     * |                        SERVER CHANNEL PIPELINE                    |
     * |                                                                   |
     * |    +----------+----------+ (3) write  +----------------------+    |
     * |    | Queue of queues     +----------->| Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               | (2) enqueue                       |               |
     * |    +----------+----------+                        |               |
     * |    | Request handler     |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | Message+Frame decoder |                       |               |
     * |   +-----------+-----------+                       |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (1) client request               \|/
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    public ChannelHandler[] getServerChannelHandlers() {
        // 分区请求队列：作为 Outbound Handler，负责将数据从队列中取出并发送
        // 内部维护多个 NetworkSequenceViewReader，每个 reader 对应一个远程消费者的请求
        PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();

        // 分区请求服务端处理器：作为 Inbound Handler，处理来自客户端的各类请求
        // 包括：PartitionRequest（建立数据通道）、AddCredit（增加发送配额）、
        // CancelPartitionRequest（取消请求）、TaskEventRequest（反向事件）等
        PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        partitionProvider, taskEventPublisher, queueOfPartitionQueues);

        // 服务端 Handler 管道顺序（从上到下为入站方向，从下到上为出站方向）：
        // 1. messageEncoder (出站)：将 NettyMessage 编码为 ByteBuf
        // 2. NettyMessageDecoder (入站)：将 ByteBuf 解码为 NettyMessage
        // 3. serverHandler (入站)：处理解码后的请求消息
        // 4. queueOfPartitionQueues (出站)：将数据从队列发送出去
        return new ChannelHandler[] {
            messageEncoder,
            new NettyMessage.NettyMessageDecoder(),
            serverHandler,
            queueOfPartitionQueues
        };
    }

    /**
     * Returns the client channel handlers.
     *
     * <pre>
     *     +-----------+----------+            +----------------------+
     *     | Remote input channel |            | request client       |
     *     +-----------+----------+            +-----------+----------+
     *                 |                                   | (1) write
     * +---------------+-----------------------------------+---------------+
     * |               |     CLIENT CHANNEL PIPELINE       |               |
     * |               |                                  \|/              |
     * |    +----------+----------+            +----------------------+    |
     * |    | Request handler     +            | Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+------------+                      |               |
     * |    | Message+Frame decoder |                      |               |
     * |    +----------+------------+                      |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (3) server response              \|/ (2) client request
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    public ChannelHandler[] getClientChannelHandlers() {
        // 客户端响应处理器：处理来自服务端的响应消息
        // 基于 Credit 机制进行流量控制，内部维护 inputChannelId -> RemoteInputChannel 的映射
        // 处理的消息类型：BufferResponse（数据）、ErrorResponse（错误）、BacklogAnnouncement（积压通知）
        NetworkClientHandler networkClientHandler = new CreditBasedPartitionRequestClientHandler();

        // 客户端 Handler 管道顺序：
        // 1. messageEncoder (出站)：将请求消息（如 PartitionRequest、AddCredit）编码发送
        // 2. NettyMessageClientDecoderDelegate (入站)：解码服务端响应，委托给 networkClientHandler
        // 3. networkClientHandler (入站)：处理解码后的响应，将数据写入 RemoteInputChannel
        return new ChannelHandler[] {
            messageEncoder,
            new NettyMessageClientDecoderDelegate(networkClientHandler),
            networkClientHandler
        };
    }
}
