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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

/**
 * Default implementation of {@link ShuffleDescriptor} for {@link NettyShuffleMaster}.
 *
 * <p>【学习型注释】NettyShuffleDescriptor 是 Shuffle 描述符的默认实现，用于描述如何连接到
 * 数据分区的生产者以进行数据交换。
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li>封装分区生产者的位置信息（哪个 TaskManager 上）</li>
 *   <li>提供网络连接信息（IP 地址、端口、连接索引）</li>
 *   <li>支持本地/远程分区的区分</li>
 *   <li>支持 TieredStorage 模式（通过 TierShuffleDescriptor）</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>JobMaster 部署 Task 时，将 ShuffleDescriptor 传递给下游消费者</li>
 *   <li>下游 Task 根据描述符创建 InputChannel 连接到上游</li>
 *   <li>根据 isLocalTo() 判断是创建 LocalInputChannel 还是 RemoteInputChannel</li>
 * </ul>
 *
 * <h2>连接信息类型</h2>
 * <ul>
 *   <li>{@link NetworkPartitionConnectionInfo}：远程连接，包含 IP+端口+连接索引</li>
 *   <li>{@link LocalExecutionPartitionConnectionInfo}：本地执行模式（MiniCluster），不需要网络</li>
 * </ul>
 */
public class NettyShuffleDescriptor implements ShuffleDescriptor {

    private static final long serialVersionUID = 852181945034989215L;

    /**
     * 分区生产者所在的 TaskManager 的 ResourceID。
     * 用于判断消费者与生产者是否在同一个 TaskManager 上（本地访问）。
     */
    private final ResourceID producerLocation;

    /**
     * 分区连接信息，包含如何连接到生产者的详细信息。
     * 可以是 NetworkPartitionConnectionInfo（远程）或 LocalExecutionPartitionConnectionInfo（本地）。
     */
    private final PartitionConnectionInfo partitionConnectionInfo;

    /**
     * 结果分区的唯一标识符，包含 IntermediateResultPartitionID 和 ExecutionAttemptID。
     */
    private final ResultPartitionID resultPartitionID;

    /**
     * TieredStorage 模式下的层级 Shuffle 描述符列表。
     * 用于 Hybrid Shuffle 的多层存储架构（内存 → 磁盘 → 远程存储）。
     */
    @Nullable private final List<TierShuffleDescriptor> tierShuffleDescriptors;

    public NettyShuffleDescriptor(
            ResourceID producerLocation,
            PartitionConnectionInfo partitionConnectionInfo,
            ResultPartitionID resultPartitionID) {
        this(producerLocation, partitionConnectionInfo, resultPartitionID, null);
    }

    public NettyShuffleDescriptor(
            ResourceID producerLocation,
            PartitionConnectionInfo partitionConnectionInfo,
            ResultPartitionID resultPartitionID,
            @Nullable List<TierShuffleDescriptor> tierShuffleDescriptors) {
        this.producerLocation = producerLocation;
        this.partitionConnectionInfo = partitionConnectionInfo;
        this.resultPartitionID = resultPartitionID;
        this.tierShuffleDescriptors = tierShuffleDescriptors;
    }

    /**
     * 获取连接 ID，用于创建到生产者的网络连接。
     *
     * <p>【学习型注释】ConnectionID 包含三个要素：
     * <ul>
     *   <li>ResourceID：目标 TaskManager 的标识</li>
     *   <li>InetSocketAddress：网络地址（IP + 端口）</li>
     *   <li>connectionIndex：复用同一物理连接时的逻辑索引</li>
     * </ul>
     */
    public ConnectionID getConnectionId() {
        return new ConnectionID(
                producerLocation,
                partitionConnectionInfo.getAddress(),
                partitionConnectionInfo.getConnectionIndex());
    }

    @Override
    public ResultPartitionID getResultPartitionID() {
        return resultPartitionID;
    }

    @Override
    public Optional<ResourceID> storesLocalResourcesOn() {
        return Optional.of(producerLocation);
    }

    public boolean isLocalTo(ResourceID consumerLocation) {
        return producerLocation.equals(consumerLocation);
    }

    @Nullable
    public List<TierShuffleDescriptor> getTierShuffleDescriptors() {
        return tierShuffleDescriptors;
    }

    /**
     * 分区连接信息接口，定义连接到分区生产者所需的信息。
     *
     * <p>【学习型注释】该接口有两个实现：
     * <ul>
     *   <li>{@link NetworkPartitionConnectionInfo}：分布式模式，需要网络地址</li>
     *   <li>{@link LocalExecutionPartitionConnectionInfo}：本地模式，无需网络</li>
     * </ul>
     */
    /** Information for connection to partition producer for shuffle exchange. */
    public interface PartitionConnectionInfo extends Serializable {
        /** 获取生产者的网络地址（IP + 数据端口） */
        InetSocketAddress getAddress();

        /** 获取连接索引，用于在同一物理连接上区分不同的逻辑通道 */
        int getConnectionIndex();
    }

    /**
     * Remote partition connection information with index to query partition.
     *
     * <p>Normal connection information with network address and port for connection in case of
     * distributed execution.
     *
     * <p>【学习型注释】远程分区连接信息，用于分布式执行模式。
     * <ul>
     *   <li>address：生产者 TaskManager 的数据服务地址（由 TaskManagerOptions.DATA_PORT 配置）</li>
     *   <li>connectionIndex：在 Netty 连接复用场景下区分不同的分区请求</li>
     * </ul>
     */
    public static class NetworkPartitionConnectionInfo implements PartitionConnectionInfo {

        private static final long serialVersionUID = 5992534320110743746L;

        /** 生产者的网络地址（IP + 数据端口） */
        private final InetSocketAddress address;

        /** 连接索引，用于标识同一连接上的不同分区请求 */
        private final int connectionIndex;

        @VisibleForTesting
        public NetworkPartitionConnectionInfo(InetSocketAddress address, int connectionIndex) {
            this.address = address;
            this.connectionIndex = connectionIndex;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public int getConnectionIndex() {
            return connectionIndex;
        }

        static NetworkPartitionConnectionInfo fromProducerDescriptor(
                ProducerDescriptor producerDescriptor, int connectionIndex) {
            InetSocketAddress address =
                    new InetSocketAddress(
                            producerDescriptor.getAddress(), producerDescriptor.getDataPort());
            return new NetworkPartitionConnectionInfo(address, connectionIndex);
        }
    }

    /**
     * Local partition connection information.
     *
     * <p>Does not have any network connection information in case of local execution.
     *
     * <p>【学习型注释】本地执行模式的分区连接信息（单例枚举）。
     * 用于 MiniCluster 等本地测试场景，所有 Task 运行在同一 JVM 中，
     * 数据交换通过内存直接传递，不需要网络连接。
     * 调用 getAddress() 或 getConnectionIndex() 会抛出 UnsupportedOperationException。
     */
    public enum LocalExecutionPartitionConnectionInfo implements PartitionConnectionInfo {
        INSTANCE;

        @Override
        public InetSocketAddress getAddress() {
            throw new UnsupportedOperationException(
                    "Local execution does not support shuffle connection.");
        }

        @Override
        public int getConnectionIndex() {
            throw new UnsupportedOperationException(
                    "Local execution does not support shuffle connection.");
        }
    }
}
