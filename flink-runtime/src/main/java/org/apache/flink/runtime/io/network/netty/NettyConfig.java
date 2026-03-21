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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.PortRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Netty 网络配置类，封装了 Flink 数据传输层（Shuffle）所需的所有 Netty 配置参数。
 *
 * <p>核心配置项：
 * <ul>
 *     <li><b>服务端地址和端口</b>：TaskManager 监听的网络地址，用于接收其他 TaskManager 的数据请求</li>
 *     <li><b>线程数</b>：Server/Client 线程数默认等于 TaskSlot 数量，确保每个 Slot 有独立的 IO 线程</li>
 *     <li><b>内存段大小</b>：网络缓冲区的基本单位，影响数据传输的粒度和效率</li>
 *     <li><b>SSL 配置</b>：支持内部数据加密传输，通过 {@code taskmanager.data.ssl.enabled} 启用</li>
 *     <li><b>TCP Keep-Alive</b>：可选的 TCP 保活配置，用于检测死连接</li>
 * </ul>
 *
 * <p>传输模式（TransportType）：
 * <ul>
 *     <li><b>NIO</b>：Java 原生 NIO，跨平台通用</li>
 *     <li><b>EPOLL</b>：Linux 专用高性能模式，需要 native 库支持</li>
 *     <li><b>AUTO</b>：自动选择，Linux 优先 EPOLL，其他平台使用 NIO</li>
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *     <li>{@link NettyClient} 和 {@link NettyServer} 初始化时读取配置</li>
 *     <li>{@link NettyConnectionManager} 创建连接管理器时传入</li>
 * </ul>
 */
public class NettyConfig {

    private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

    /**
     * Netty 传输类型枚举。
     * <ul>
     *     <li>NIO：基于 Java NIO 的跨平台实现</li>
     *     <li>EPOLL：Linux 平台的高性能实现，直接调用 epoll 系统调用</li>
     *     <li>AUTO：自动选择，Linux 优先 EPOLL</li>
     * </ul>
     */
    enum TransportType {
        NIO,
        EPOLL,
        AUTO
    }

    /** Netty Server 端线程组名称，便于 JVM 工具识别和调试 */
    static final String SERVER_THREAD_GROUP_NAME = "Flink Netty Server";

    /** Netty Client 端线程组名称 */
    static final String CLIENT_THREAD_GROUP_NAME = "Flink Netty Client";

    /** TaskManager 监听的网络地址，其他 TM 通过此地址请求数据 */
    private final InetAddress serverAddress;

    /** 服务端口范围，支持指定端口或端口范围（用于避免端口冲突） */
    private final PortRange serverPortRange;

    /** 网络缓冲区内存段大小（字节），通常与 MemorySegment 大小一致，默认 32KB */
    private final int memorySegmentSize;

    /** TaskSlot 数量，决定 Netty 线程池大小（Server/Client 线程数 = Slot 数） */
    private final int numberOfSlots;

    /** Flink 配置对象，用于读取 Netty 相关的可选配置项 */
    private final Configuration config;

    public NettyConfig(
            InetAddress serverAddress,
            int serverPort,
            int memorySegmentSize,
            int numberOfSlots,
            Configuration config) {
        this(serverAddress, new PortRange(serverPort), memorySegmentSize, numberOfSlots, config);
    }

    public NettyConfig(
            InetAddress serverAddress,
            PortRange serverPortRange,
            int memorySegmentSize,
            int numberOfSlots,
            Configuration config) {

        this.serverAddress = checkNotNull(serverAddress);
        this.serverPortRange = serverPortRange;

        checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
        this.memorySegmentSize = memorySegmentSize;

        checkArgument(numberOfSlots > 0, "Number of slots");
        this.numberOfSlots = numberOfSlots;

        this.config = checkNotNull(config);

        LOG.info(this.toString());
    }

    InetAddress getServerAddress() {
        return serverAddress;
    }

    PortRange getServerPortRange() {
        return serverPortRange;
    }

    // ------------------------------------------------------------------------
    // Getters
    // ------------------------------------------------------------------------

    /**
     * 获取服务端 Socket 连接等待队列大小（backlog）。
     * 返回 0 表示使用 Netty 默认值（通常为 128）。
     */
    public int getServerConnectBacklog() {
        return 0;
    }

    /**
     * 获取 Netty 内存分配器的 Arena 数量。
     * 每个 TaskSlot 对应一个 Arena，避免多线程竞争。
     */
    public int getNumberOfArenas() {
        // always return number of task slots
        return numberOfSlots;
    }

    /**
     * 获取 Netty Server 端线程数。
     * 默认等于 TaskSlot 数量，确保每个 Slot 有独立的 IO 线程处理入站请求。
     */
    public int getServerNumThreads() {
        // always return number of task slots
        return numberOfSlots;
    }

    /**
     * 获取 Netty Client 端线程数。
     * 默认等于 TaskSlot 数量，确保每个 Slot 有独立的 IO 线程处理出站请求。
     */
    public int getClientNumThreads() {
        // always return number of task slots
        return numberOfSlots;
    }

    /**
     * 获取 Client 连接超时时间（秒）。
     * 超过此时间未建立连接将抛出 RemoteTransportException。
     */
    public int getClientConnectTimeoutSeconds() {
        return config.get(NettyShuffleEnvironmentOptions.CLIENT_CONNECT_TIMEOUT_SECONDS);
    }

    /**
     * 获取网络重试次数。
     * 连接失败或数据传输失败时的重试次数。
     */
    public int getNetworkRetries() {
        return config.get(NettyShuffleEnvironmentOptions.NETWORK_RETRIES);
    }

    /**
     * 获取 Socket 发送/接收缓冲区大小。
     * 返回 0 表示使用操作系统默认值。
     */
    public int getSendAndReceiveBufferSize() {
        return 0;
    }

    /** 获取 TCP Keep-Alive 空闲时间（秒），连接空闲超过此时间后发送探测包 */
    public Optional<Integer> getTcpKeepIdleInSeconds() {
        return config.getOptional(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_IDLE_SECONDS);
    }

    /** 获取 TCP Keep-Alive 探测间隔（秒），两次探测包之间的间隔 */
    public Optional<Integer> getTcpKeepInternalInSeconds() {
        return config.getOptional(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_INTERVAL_SECONDS);
    }

    /** 获取 TCP Keep-Alive 探测次数，超过此次数未响应则认为连接断开 */
    public Optional<Integer> getTcpKeepCount() {
        return config.getOptional(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_COUNT);
    }

    /**
     * 创建 Client 端 SSL 引擎工厂。
     * 仅当 SSL 启用时返回非空工厂，用于加密出站连接。
     */
    @Nullable
    public SSLHandlerFactory createClientSSLEngineFactory() throws Exception {
        return getSSLEnabled() ? SSLUtils.createInternalClientSSLEngineFactory(config) : null;
    }

    /**
     * 创建 Server 端 SSL 引擎工厂。
     * 仅当 SSL 启用时返回非空工厂，用于加密入站连接。
     */
    @Nullable
    public SSLHandlerFactory createServerSSLEngineFactory() throws Exception {
        return getSSLEnabled() ? SSLUtils.createInternalServerSSLEngineFactory(config) : null;
    }

    /**
     * 判断 SSL 是否启用。
     * 需要同时满足：数据 SSL 启用 AND 内部 SSL 启用。
     */
    public boolean getSSLEnabled() {
        return config.get(NettyShuffleEnvironmentOptions.DATA_SSL_ENABLED)
                && SecurityOptions.isInternalSSLEnabled(config);
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public String toString() {
        String format =
                "NettyConfig ["
                        + "server address: %s, "
                        + "server port range: %s, "
                        + "ssl enabled: %s, "
                        + "memory segment size (bytes): %d, "
                        + "number of server threads: %d (%s), "
                        + "number of client threads: %d (%s), "
                        + "server connect backlog: %d (%s), "
                        + "client connect timeout (sec): %d, "
                        + "send/receive buffer size (bytes): %d (%s)]";

        String def = "use Netty's default";
        String man = "manual";

        return String.format(
                format,
                serverAddress,
                serverPortRange,
                getSSLEnabled() ? "true" : "false",
                memorySegmentSize,
                getServerNumThreads(),
                getServerNumThreads() == 0 ? def : man,
                getClientNumThreads(),
                getClientNumThreads() == 0 ? def : man,
                getServerConnectBacklog(),
                getServerConnectBacklog() == 0 ? def : man,
                getClientConnectTimeoutSeconds(),
                getSendAndReceiveBufferSize(),
                getSendAndReceiveBufferSize() == 0 ? def : man);
    }
}
