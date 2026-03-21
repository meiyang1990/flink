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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Netty 连接管理器 - ConnectionManager 的 Netty 实现，管理网络 Shuffle 的客户端和服务端连接。
 *
 * <p>核心职责：
 * <ul>
 *   <li>持有并管理 NettyServer：接收远程消费者的数据请求，发送 ResultPartition 数据</li>
 *   <li>持有并管理 NettyClient：向远程生产者发起连接，请求 ResultPartition 数据</li>
 *   <li>提供 PartitionRequestClient 工厂：为每个远程连接创建可复用的请求客户端</li>
 *   <li>管理 Netty 内存池：统一的 ByteBuf 分配，减少内存碎片</li>
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>TaskManager 启动时创建，用于支持任务间的网络数据交换</li>
 *   <li>RemoteInputChannel 通过此管理器获取 PartitionRequestClient 发起数据请求</li>
 *   <li>ResultPartition 的数据通过此管理器的 Server 端发送给远程消费者</li>
 * </ul>
 *
 * <p>组件架构：
 * <pre>
 *   NettyConnectionManager
 *     ├── NettyServer          // 服务端，监听端口接收请求
 *     ├── NettyClient          // 客户端，发起连接
 *     ├── NettyBufferPool      // Netty 内存池
 *     ├── PartitionRequestClientFactory  // 请求客户端工厂（支持连接复用）
 *     └── NettyProtocol        // 协议定义（Handler 配置）
 * </pre>
 *
 * <p>连接复用机制：
 * <ul>
 *   <li>多个 RemoteInputChannel 到同一 TaskManager 可共享同一个 TCP 连接</li>
 *   <li>通过 connectionReuseEnabled 配置开启/关闭连接复用</li>
 *   <li>复用时通过引用计数管理连接生命周期</li>
 * </ul>
 */
public class NettyConnectionManager implements ConnectionManager {

    /** Netty 服务端：监听端口，接收远程消费者的 PartitionRequest，发送数据 */
    private final NettyServer server;

    /** Netty 客户端：发起到远程 TaskManager 的连接，用于请求 ResultPartition 数据 */
    private final NettyClient client;

    /** Netty 内存池：提供统一的 ByteBuf 分配，配置的 arena 数量影响并发性能 */
    private final NettyBufferPool bufferPool;

    /**
     * 分区请求客户端工厂：管理到远程 TaskManager 的连接和请求客户端。
     * 支持连接复用，避免为每个 InputChannel 创建新连接。
     */
    private final PartitionRequestClientFactory partitionRequestClientFactory;

    /** Netty 协议定义：包含服务端和客户端的 ChannelHandler 配置 */
    private final NettyProtocol nettyProtocol;

    /**
     * 构造 NettyConnectionManager（生产环境使用）。
     * 自动创建 NettyBufferPool，arena 数量由配置决定。
     *
     * @param partitionProvider 结果分区提供者，服务端用于获取数据
     * @param taskEventPublisher Task 事件发布者
     * @param nettyConfig Netty 配置（端口、线程数、内存等）
     * @param connectionReuseEnabled 是否启用连接复用
     */
    public NettyConnectionManager(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            boolean connectionReuseEnabled) {

        this(
                new NettyBufferPool(nettyConfig.getNumberOfArenas()),
                partitionProvider,
                taskEventPublisher,
                nettyConfig,
                connectionReuseEnabled);
    }

    /**
     * 构造 NettyConnectionManager（测试用，允许注入 bufferPool）。
     */
    @VisibleForTesting
    public NettyConnectionManager(
            NettyBufferPool bufferPool,
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            boolean connectionReuseEnabled) {

        this.server = new NettyServer(nettyConfig);
        this.client = new NettyClient(nettyConfig);
        this.bufferPool = checkNotNull(bufferPool);

        // 创建请求客户端工厂，配置重试次数和连接复用策略
        this.partitionRequestClientFactory =
                new PartitionRequestClientFactory(
                        client, nettyConfig.getNetworkRetries(), connectionReuseEnabled);

        // 创建协议定义，封装服务端和客户端的 Handler 配置
        this.nettyProtocol =
                new NettyProtocol(
                        checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));
    }

    /**
     * 启动服务端和客户端。
     * 先初始化客户端（配置 Bootstrap），再初始化服务端（绑定端口）。
     *
     * @return 服务端监听的实际端口号
     */
    @Override
    public int start() throws IOException {
        // 初始化客户端：配置 Bootstrap，设置 Handler 管道
        client.init(nettyProtocol, bufferPool);

        // 初始化服务端：绑定端口，返回实际端口号
        return server.init(nettyProtocol, bufferPool);
    }

    /**
     * 为指定的远程连接创建或获取 PartitionRequestClient。
     * 如果启用连接复用，会尝试复用已有连接。
     *
     * @param connectionId 远程连接标识（包含地址和连接索引）
     * @return 可用于发送请求的 PartitionRequestClient
     */
    @Override
    public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
    }

    /**
     * 关闭指定远程地址的所有打开通道。
     * 用于在连接异常或 TaskManager 下线时清理连接。
     */
    @Override
    public void closeOpenChannelConnections(ConnectionID connectionId) {
        partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
    }

    /** 返回当前活跃的客户端连接数 */
    @Override
    public int getNumberOfActiveConnections() {
        return partitionRequestClientFactory.getNumberOfActiveClients();
    }

    /** 关闭服务端和客户端，释放资源 */
    @Override
    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }

    NettyClient getClient() {
        return client;
    }

    NettyServer getServer() {
        return server;
    }

    NettyBufferPool getBufferPool() {
        return bufferPool;
    }
}
