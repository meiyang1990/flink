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

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;

/**
 * Interface for shuffle service factory implementations.
 *
 * <p>This component is a light-weight factory for {@link ShuffleMaster} and {@link
 * ShuffleEnvironment}.
 *
 * <p>Shuffle 服务工厂接口，是创建 {@link ShuffleMaster} 和 {@link ShuffleEnvironment} 的轻量级工厂。
 *
 * <h2>设计目的</h2>
 * <p>该接口是 Flink Shuffle 服务的插件化入口点，允许用户自定义 Shuffle 实现。
 * 通过 SPI（Service Provider Interface）机制加载，用户只需实现该接口并在
 * META-INF/services 中注册即可替换默认的 Netty Shuffle。
 *
 * <h2>组件关系</h2>
 * <pre>
 *                    ShuffleServiceFactory
 *                           │
 *          ┌────────────────┼────────────────┐
 *          ↓                                 ↓
 *    ShuffleMaster                    ShuffleEnvironment
 *    (JobManager 端)                  (TaskManager 端)
 *          │                                 │
 *          │                                 ├── ResultPartitionWriter
 *          └── ShuffleDescriptor ──────────→│
 *                                            └── IndexedInputGate
 * </pre>
 *
 * <h2>内置实现</h2>
 * <ul>
 *   <li>{@code NettyShuffleServiceFactory}：基于 Netty 的默认实现，支持流式和批处理</li>
 * </ul>
 *
 * <h2>泛型参数</h2>
 * <ul>
 *   <li>SD：ShuffleDescriptor 子类，描述分区位置和连接信息</li>
 *   <li>P：ResultPartitionWriter 子类，数据生产端写入器</li>
 *   <li>G：IndexedInputGate 子类，数据消费端输入门</li>
 * </ul>
 *
 * <h2>配置方式</h2>
 * <p>通过 {@code shuffle-service-factory.class} 配置项指定自定义实现类。
 *
 * @param <SD> partition shuffle descriptor used for producer/consumer deployment and their data
 *     exchange.
 * @param <P> type of provided result partition writers
 * @param <G> type of provided input gates
 *
 * @see ShuffleMaster
 * @see ShuffleEnvironment
 */
public interface ShuffleServiceFactory<
        SD extends ShuffleDescriptor, P extends ResultPartitionWriter, G extends IndexedInputGate> {

    /**
     * 创建 ShuffleMaster 实例。
     *
     * <p>ShuffleMaster 运行在 JobManager 端，负责：
     * <ul>
     *   <li>分区注册：接收 TaskManager 上报的分区信息</li>
     *   <li>生成 ShuffleDescriptor：为消费端提供分区位置信息</li>
     *   <li>协调分区生命周期：处理分区释放和故障恢复</li>
     * </ul>
     *
     * @param shuffleMasterContext ShuffleMaster 上下文，提供配置和回调
     * @return ShuffleMaster 实例
     */
    ShuffleMaster<SD> createShuffleMaster(ShuffleMasterContext shuffleMasterContext);

    /**
     * 创建本地 ShuffleEnvironment 实例。
     *
     * <p>ShuffleEnvironment 运行在 TaskManager 端，负责：
     * <ul>
     *   <li>创建 ResultPartitionWriter：数据生产端用于写出数据</li>
     *   <li>创建 IndexedInputGate：数据消费端用于读取数据</li>
     *   <li>管理网络资源：缓冲池、连接等</li>
     *   <li>处理数据交换：本地和远程的数据传输</li>
     * </ul>
     *
     * @param shuffleEnvironmentContext 本地 Shuffle 环境上下文
     * @return ShuffleEnvironment 实例
     */
    ShuffleEnvironment<P, G> createShuffleEnvironment(
            ShuffleEnvironmentContext shuffleEnvironmentContext);
}
