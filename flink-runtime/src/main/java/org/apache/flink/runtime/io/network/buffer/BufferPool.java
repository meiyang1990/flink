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

package org.apache.flink.runtime.io.network.buffer;

import java.io.IOException;

/**
 * A dynamically sized buffer pool.
 *
 * <p>动态大小的缓冲池接口，继承 {@link BufferProvider}（提供缓冲区）和 {@link BufferRecycler}（回收缓冲区）。
 *
 * <h2>设计目的</h2>
 * <p>BufferPool 是 Flink 网络栈中的本地缓冲池抽象，每个 Task 的输入/输出 Gate 都拥有独立的 BufferPool 实例，
 * 从全局的 {@link NetworkBufferPool} 动态借用和归还内存段。
 *
 * <h2>两级缓冲池架构</h2>
 * <pre>
 *     Task A                      Task B
 *        ↓                           ↓
 *  LocalBufferPool A          LocalBufferPool B
 *        ↘                         ↙
 *          NetworkBufferPool (全局)
 *               ↓
 *          MemorySegment[]
 * </pre>
 *
 * <h2>核心配置参数</h2>
 * <ul>
 *   <li><b>requiredMemorySegments</b>：保证的最小缓冲区数量，必须满足</li>
 *   <li><b>maxMemorySegments</b>：最大缓冲区数量，-1 表示无限制</li>
 *   <li><b>numBuffers</b>：当前分配的缓冲区数量，可动态调整</li>
 * </ul>
 *
 * <h2>实现类</h2>
 * <ul>
 *   <li>{@link LocalBufferPool}：标准实现，支持 Credit-based 流量控制</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>ResultPartition：生产端使用 BufferPool 分配写入缓冲区</li>
 *   <li>InputGate：消费端使用 BufferPool 分配接收缓冲区</li>
 * </ul>
 *
 * @see LocalBufferPool
 * @see NetworkBufferPool
 */
public interface BufferPool extends BufferProvider, BufferRecycler {

    /**
     * 预留指定数量的内存段到该缓冲池。
     *
     * <p>如果无法分配足够的内存段将抛出异常。这是一个阻塞操作，
     * 用于在 Task 启动前确保最小缓冲区数量。
     *
     * @param numberOfSegmentsToReserve 需要预留的内存段数量
     * @throws IOException 如果无法分配足够的内存段
     */
    void reserveSegments(int numberOfSegmentsToReserve) throws IOException;

    /**
     * 延迟销毁该缓冲池。
     *
     * <p>如果有缓冲区尚未归还，它们会在回收时被延迟销毁。
     * 这种设计避免了必须等待所有缓冲区回收的问题，提高了资源释放的效率。
     */
    void lazyDestroy();

    /**
     * 检查该缓冲池是否已被销毁。
     *
     * @return true 如果缓冲池已销毁
     */
    @Override
    boolean isDestroyed();

    /**
     * 获取该缓冲池保证的最小内存段数量。
     *
     * <p>这个数量是必须满足的，如果全局缓冲池无法提供，会导致 Task 启动失败。
     *
     * @return 保证的最小内存段数量
     */
    int getNumberOfRequiredMemorySegments();

    /**
     * 获取该缓冲池允许使用的最大内存段数量。
     *
     * @return 最大内存段数量，-1 表示无限制
     */
    int getMaxNumberOfMemorySegments();

    /**
     * 获取当前缓冲池的大小（当前分配的缓冲区数量）。
     *
     * <p>缓冲池大小可以在运行时动态调整，以适应不同的负载情况。
     *
     * @return 当前缓冲池大小
     */
    int getNumBuffers();

    /**
     * 设置当前缓冲池的大小。
     *
     * <p>新的大小必须大于等于保证的最小内存段数量。
     * 这个方法允许动态调整缓冲池大小以实现更好的内存利用。
     *
     * @param numBuffers 新的缓冲池大小
     */
    void setNumBuffers(int numBuffers);

    /**
     * 设置每个 Gate 允许的最大透支缓冲区数量。
     *
     * <p>透支缓冲区是指超出正常分配但仍允许使用的额外缓冲区，
     * 用于处理突发流量避免数据丢失。
     *
     * @param maxOverdraftBuffersPerGate 最大透支缓冲区数量
     */
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /**
     * 获取每个 Gate 允许的最大透支缓冲区数量。
     *
     * @return 最大透支缓冲区数量
     */
    int getMaxOverdraftBuffersPerGate();

    /**
     * 获取当前缓冲池持有的可用内存段数量。
     *
     * <p>可用内存段是指已分配但尚未被使用的缓冲区。
     *
     * @return 可用内存段数量
     */
    int getNumberOfAvailableMemorySegments();

    /**
     * 尽力获取已使用的缓冲区数量。
     *
     * <p>这是一个非精确的统计方法，可能在高并发场景下有轻微误差，
     * 但避免了加锁开销，适用于监控和调试目的。
     *
     * @return 已使用的缓冲区数量（近似值）
     */
    int bestEffortGetNumOfUsedBuffers();

    /**
     * 获取指定 Channel 请求的缓冲区数量。
     *
     * <p>用于 Credit-based 流量控制中追踪每个 Channel 的缓冲区使用情况。
     *
     * @param targetChannel 目标 Channel 索引
     * @return 该 Channel 的缓冲区数量，默认返回 0
     */
    default int getBuffersCountUnsafe(int targetChannel) {
        return 0;
    }
}
