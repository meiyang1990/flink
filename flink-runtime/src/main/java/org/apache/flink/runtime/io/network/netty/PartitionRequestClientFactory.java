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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Factory for {@link NettyPartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 *
 * <p>分区请求客户端工厂，负责创建和管理 {@link NettyPartitionRequestClient} 实例。
 *
 * <p>核心职责：
 * <ul>
 *     <li><b>连接复用</b>：同一 TaskManager 的多个 RemoteInputChannel 共享同一个 Client，
 *         通过 ConnectionID 作为 key 进行缓存，避免重复建立 TCP 连接</li>
 *     <li><b>连接创建</b>：通过 {@link NettyClient#connect} 建立 TCP 连接，
 *         支持重试机制（retryNumber 配置）</li>
 *     <li><b>引用计数</b>：Client 通过引用计数管理生命周期，最后一个 InputChannel 释放时关闭连接</li>
 *     <li><b>并发安全</b>：使用 ConcurrentHashMap + CompletableFuture 保证并发创建时只建立一个连接</li>
 * </ul>
 *
 * <p>连接创建流程：
 * <pre>
 *     createPartitionRequestClient(connectionId)
 *           │
 *           ├── 查询缓存 clients.get(connectionId)
 *           │         │
 *           │         ├── 缓存命中 → 等待 Future 完成 → 增加引用计数 → 返回 Client
 *           │         │
 *           │         └── 缓存未命中 → putIfAbsent 占位 → connectWithRetries
 *           │                                                    │
 *           │                                                    ├── 成功 → complete(client)
 *           │                                                    │
 *           │                                                    └── 失败 → completeExceptionally → 移除缓存
 *           │
 *           └── 验证 Client 有效性 → 无效则销毁并重试
 * </pre>
 */
class PartitionRequestClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientFactory.class);

    /** 底层 Netty 客户端，用于建立 TCP 连接 */
    private final NettyClient nettyClient;

    /** 连接失败时的重试次数，0 表示不重试 */
    private final int retryNumber;

    /**
     * Client 缓存，key 为 ConnectionID（目标 TM 地址）。
     * 使用 CompletableFuture 作为 value，支持并发场景下只创建一个连接：
     * - 第一个线程 putIfAbsent 成功，负责创建连接
     * - 其他线程等待 Future 完成，获取已创建的 Client
     */
    private final ConcurrentMap<ConnectionID, CompletableFuture<NettyPartitionRequestClient>>
            clients = new ConcurrentHashMap<>();

    /** 是否启用连接复用，关闭时每次请求都创建新连接（主要用于测试） */
    private final boolean connectionReuseEnabled;

    PartitionRequestClientFactory(NettyClient nettyClient, boolean connectionReuseEnabled) {
        this(nettyClient, 0, connectionReuseEnabled);
    }

    PartitionRequestClientFactory(
            NettyClient nettyClient, int retryNumber, boolean connectionReuseEnabled) {
        this.nettyClient = nettyClient;
        this.retryNumber = retryNumber;
        this.connectionReuseEnabled = connectionReuseEnabled;
    }

    /**
     * Atomically establishes a TCP connection to the given remote address and creates a {@link
     * NettyPartitionRequestClient} instance for this connection.
     *
     * <p>原子性地建立到远程地址的 TCP 连接，并创建对应的请求客户端。
     *
     * <p>关键实现细节：
     * <ul>
     *     <li>ConnectionID 重新映射：将原始 connectionId 的索引设为 0，
     *         确保同一目标 TM 只创建一个 TCP 连接</li>
     *     <li>使用 putIfAbsent + CompletableFuture 保证并发安全</li>
     *     <li>引用计数验证：返回 Client 前必须成功增加引用计数，
     *         防止返回已关闭的 Client</li>
     * </ul>
     */
    NettyPartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        // 重新映射 ConnectionID，将索引设为 0 以限制到同一 TM 的 TCP 连接数为 1
        connectionId = new ConnectionID(connectionId.getResourceID(), connectionId.getAddress(), 0);
        while (true) {
            // 创建新的 Future 作为占位符
            final CompletableFuture<NettyPartitionRequestClient> newClientFuture =
                    new CompletableFuture<>();

            // 尝试原子性地放入缓存，如果已存在则返回已有的 Future
            final CompletableFuture<NettyPartitionRequestClient> clientFuture =
                    clients.putIfAbsent(connectionId, newClientFuture);

            final NettyPartitionRequestClient client;

            if (clientFuture == null) {
                // putIfAbsent 返回 null 说明当前线程是第一个，负责创建连接
                try {
                    client = connectWithRetries(connectionId);
                } catch (Throwable e) {
                    // 连接失败，通知等待的线程并移除缓存
                    newClientFuture.completeExceptionally(
                            new IOException("Could not create Netty client.", e));
                    clients.remove(connectionId, newClientFuture);
                    throw e;
                }

                // 连接成功，通知等待的线程
                newClientFuture.complete(client);
            } else {
                // 其他线程已在创建，等待其完成
                try {
                    client = clientFuture.get();
                } catch (ExecutionException e) {
                    ExceptionUtils.rethrowIOException(ExceptionUtils.stripExecutionException(e));
                    return null;
                }
            }

            // 验证 Client 有效性并增加引用计数
            // 增加引用计数确保在使用期间 Client 不会被关闭
            if (client.validateClientAndIncrementReferenceCounter()) {
                return client;
            } else if (client.canBeDisposed()) {
                // Client 无效且可以销毁，直接关闭连接
                client.closeConnection();
            } else {
                // Client 无效但不能销毁（可能有其他线程在使用），从缓存移除
                destroyPartitionRequestClient(connectionId, client);
            }
            // 循环重试，创建新连接
        }
    }

    public boolean isConnectionReuseEnabled() {
        return connectionReuseEnabled;
    }

    /**
     * 带重试的连接方法。
     * 连接失败时会重试 retryNumber 次，超过重试次数后抛出 RemoteTransportException。
     */
    private NettyPartitionRequestClient connectWithRetries(ConnectionID connectionId)
            throws InterruptedException, RemoteTransportException {
        int tried = 0;
        while (true) {
            try {
                return connect(connectionId);
            } catch (RemoteTransportException e) {
                tried++;
                if (tried > retryNumber) {
                    LOG.warn("Failed to connect to {}. Giving up.", connectionId.getAddress(), e);
                    throw e;
                } else {
                    LOG.warn(
                            "Failed {} times to connect to {}. Retrying.",
                            tried,
                            connectionId.getAddress(),
                            e);
                }
            }
        }
    }

    /**
     * 建立到目标 TaskManager 的 TCP 连接。
     *
     * <p>使用 sync() 而非 await() 的原因：
     * sync() 会等待连接完成并在失败时抛出异常，
     * 而 await() 只是等待完成，不会抛出异常。
     */
    private NettyPartitionRequestClient connect(ConnectionID connectionId)
            throws RemoteTransportException, InterruptedException {
        try {
            // 建立 TCP 连接，sync() 阻塞直到连接完成
            Channel channel = nettyClient.connect(connectionId.getAddress()).sync().channel();
            // 从 Pipeline 中获取 ClientHandler，用于发送请求和处理响应
            NetworkClientHandler clientHandler = channel.pipeline().get(NetworkClientHandler.class);
            return new NettyPartitionRequestClient(channel, clientHandler, connectionId, this);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            // 连接失败，通常表示远程 TaskManager 已丢失
            throw new RemoteTransportException(
                    "Connecting to remote task manager '"
                            + connectionId.getAddress()
                            + " [ "
                            + connectionId.getResourceID().getStringWithMetadata()
                            + " ] "
                            + "' has failed. This might indicate that the remote task "
                            + "manager has been lost.",
                    connectionId.getAddress(),
                    e);
        }
    }

    /**
     * 关闭指定 ConnectionID 对应的开放连接。
     * 如果连接创建尚未完成，会等待完成后再检查是否可以关闭。
     */
    void closeOpenChannelConnections(ConnectionID connectionId) {
        CompletableFuture<NettyPartitionRequestClient> entry = clients.get(connectionId);

        if (entry != null && !entry.isDone()) {
            // 连接尚未完成，等待完成后检查是否可以销毁
            entry.thenAccept(
                    client -> {
                        if (client.canBeDisposed()) {
                            clients.remove(connectionId, entry);
                        }
                    });
        }
    }

    /** 获取当前活跃的 Client 数量 */
    int getNumberOfActiveClients() {
        return clients.size();
    }

    /**
     * 从缓存中移除指定的 Client。
     * 仅当缓存中的 Client 与传入的 Client 相同时才移除，避免误删新创建的 Client。
     */
    void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
        final CompletableFuture<NettyPartitionRequestClient> future = clients.get(connectionId);
        if (future != null && future.isDone()) {
            future.thenAccept(
                    futureClient -> {
                        // 只有当缓存中的 Client 与传入的相同时才移除
                        if (client.equals(futureClient)) {
                            clients.remove(connectionId, future);
                        }
                    });
        }
    }
}
