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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Data of different subpartitions can be appended to a {@link DataBuffer} and after the {@link
 * DataBuffer} is full or finished, the appended data can be copied from it in subpartition index
 * order.
 *
 * <p>The lifecycle of a {@link DataBuffer} can be: new, write, [read, reset, write], finish, read,
 * release. There can be multiple [read, reset, write] operations before finish.
 *
 * <h2>核心设计概述</h2>
 * <p>DataBuffer 是 Flink Sort-Merge Blocking Shuffle 机制中的核心抽象接口，用于在内存中临时缓存
 * 多个子分区（subpartition）的数据。其核心设计要点：
 *
 * <h3>1. 数据组织方式</h3>
 * <ul>
 *   <li><b>多子分区数据混合写入</b>：不同子分区的数据可以交错写入同一个 DataBuffer</li>
 *   <li><b>按子分区索引顺序读取</b>：读取时数据会按子分区索引顺序输出，便于后续写入磁盘</li>
 *   <li><b>记录顺序保证</b>：同一子分区内的记录保持写入顺序</li>
 * </ul>
 *
 * <h3>2. 生命周期状态机</h3>
 * <pre>
 *   +-------+     +-------+     +--------+     +------+     +---------+
 *   |  NEW  | --> | WRITE | --> | FINISH | --> | READ | --> | RELEASE |
 *   +-------+     +-------+     +--------+     +------+     +---------+
 *                    ^  |
 *                    |  v (可选的重复循环)
 *                 +-------+
 *                 | RESET |
 *                 +-------+
 * </pre>
 *
 * <h3>3. 实现类</h3>
 * <ul>
 *   <li>{@link HashBasedDataBuffer}：基于 Hash 分桶，每个内存段只包含单个子分区的数据</li>
 *   <li>{@link SortBasedDataBuffer}：基于排序索引，数据连续存储，通过索引链表按子分区组织</li>
 * </ul>
 *
 * <h3>4. 使用场景</h3>
 * <ul>
 *   <li>批处理作业的 Shuffle 数据暂存</li>
 *   <li>Sort-Merge 写入前的内存缓冲</li>
 *   <li>减少随机 I/O，实现顺序写入优化</li>
 * </ul>
 *
 * @see HashBasedDataBuffer
 * @see SortBasedDataBuffer
 * @see SortMergeResultPartition
 */
public interface DataBuffer {

    /**
     * Appends data of the specified subpartition to this {@link DataBuffer} and returns true if
     * this {@link DataBuffer} is full.
     *
     * <p>向指定子分区追加数据。核心写入方法，支持记录（DATA_BUFFER）和事件（EVENT_BUFFER）两种类型。
     *
     * @param source 待写入的数据，来自序列化后的记录或事件
     * @param targetSubpartition 目标子分区索引，决定数据归属哪个下游消费者
     * @param dataType 数据类型，用于区分普通记录和事件（如 Checkpoint Barrier）
     * @return 如果 DataBuffer 已满无法继续写入则返回 true，调用者需要 finish 并开始新的 DataBuffer
     */
    boolean append(ByteBuffer source, int targetSubpartition, Buffer.DataType dataType)
            throws IOException;

    /**
     * Copies data in this {@link DataBuffer} to the target {@link MemorySegment} in subpartition
     * index order and returns {@link BufferWithSubpartition} which contains the copied data and the
     * corresponding subpartition index.
     *
     * <p>按子分区顺序读取下一个缓冲区数据。读取时会自动按子分区索引排序输出，
     * 实现 Sort-Merge Shuffle 的"先排序后写入"语义。
     *
     * @param transitBuffer 用于承载读取数据的目标内存段，可为 null 表示由实现类自行分配
     * @return 包含缓冲区数据和对应子分区索引的封装对象，无更多数据时返回 null
     */
    BufferWithSubpartition getNextBuffer(@Nullable MemorySegment transitBuffer);

    /**
     * Returns the total number of records written to this {@link DataBuffer}.
     *
     * <p>返回已写入的记录总数，用于统计和监控。
     */
    long numTotalRecords();

    /**
     * Returns the total number of bytes written to this {@link DataBuffer}.
     *
     * <p>返回已写入的字节总数，用于判断缓冲区容量和统计。
     */
    long numTotalBytes();

    /**
     * Returns true if not all data appended to this {@link DataBuffer} is consumed.
     *
     * <p>判断是否还有未读取的数据，用于读取循环的终止条件。
     */
    boolean hasRemaining();

    /**
     * Finishes this {@link DataBuffer} which means no record can be appended anymore.
     *
     * <p>完成写入阶段，进入可读取状态。调用后不能再追加数据，但可以开始读取。
     */
    void finish();

    /**
     * Whether this {@link DataBuffer} is finished or not.
     *
     * <p>检查是否已完成写入，finished 状态后才能进行读取操作。
     */
    boolean isFinished();

    /**
     * Releases this {@link DataBuffer} which releases all resources.
     *
     * <p>释放所有资源，包括内存段、缓冲区等。释放后不能再使用此 DataBuffer。
     */
    void release();

    /**
     * Whether this {@link DataBuffer} is released or not.
     *
     * <p>检查是否已释放，已释放的 DataBuffer 不能再进行任何操作。
     */
    boolean isReleased();
}
