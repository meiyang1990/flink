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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.function.TriConsumer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Accumulates received records into buffers. The {@link BufferAccumulator} receives the records
 * from tiered store producer and the records will accumulate and transform into buffers.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>缓冲区累积器，负责将从 Tiered Storage 生产者接收的记录累积并转换为完整的缓冲区。
 *
 * <h3>数据流程</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │                   TieredStorageProducerClient               │
 * │                           │                                 │
 * │                           ▼ write()                         │
 * │  ┌─────────────────────────────────────────────────────┐   │
 * │  │              BufferAccumulator                       │   │
 * │  │  ┌─────────────────────────────────────────────────┐ │   │
 * │  │  │  receive(record, subpartitionId, dataType)      │ │   │
 * │  │  │      │                                          │ │   │
 * │  │  │      ▼ 累积到子分区缓冲区                        │ │   │
 * │  │  │  ┌───────────────────────────────────────────┐  │ │   │
 * │  │  │  │ SubpartitionBuffer (per subpartition)     │  │ │   │
 * │  │  │  └───────────────────────────────────────────┘  │ │   │
 * │  │  │      │ 缓冲区写满                               │ │   │
 * │  │  │      ▼                                          │ │   │
 * │  │  │  bufferFlusher.accept(subpartitionId, buffer)   │ │   │
 * │  │  └─────────────────────────────────────────────────┘ │   │
 * │  └─────────────────────────────────────────────────────────┘│
 * │                           │                                 │
 * │                           ▼                                 │
 * │                   TierProducerAgent                         │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link HashBufferAccumulator}: 基于哈希的累积器，每个子分区独立缓冲
 *   <li>{@link SortBufferAccumulator}: 基于排序的累积器，用于批处理场景
 * </ul>
 */
public interface BufferAccumulator extends AutoCloseable {

    /**
     * Setup the accumulator.
     *
     * <p>初始化累积器，注册缓冲区刷出回调。
     *
     * @param bufferFlusher accepts the accumulated buffers. The first field is the subpartition id,
     *     the second is the accumulated buffer to flush, and the third is the number of remaining
     *     buffers to be written consecutively to the same segment.
     *     回调参数说明：
     *     <ul>
     *       <li>第一个参数：子分区 ID
     *       <li>第二个参数：待刷出的缓冲区
     *       <li>第三个参数：同一 Segment 中剩余待连续写入的缓冲区数
     *     </ul>
     */
    void setup(TriConsumer<TieredStorageSubpartitionId, Buffer, Integer> bufferFlusher);

    /**
     * Receives the records from tiered store producer, these records will be accumulated and
     * transformed into finished buffers.
     *
     * <p>Note that when isBroadcast is true, for a broadcast-only partition, the subpartitionId
     * value will always be 0. Conversely, for a non-broadcast-only partition, the subpartitionId
     * value will range from 0 to the number of subpartitions.
     *
     * <p>接收记录并累积到对应子分区的缓冲区中。当缓冲区写满时会触发刷出。
     *
     * @param record the received record（接收的记录数据）
     * @param subpartitionId the subpartition id of the record（目标子分区 ID）
     * @param dataType the data type of the record（数据类型：DATA/EVENT 等）
     * @param isBroadcast whether the record is a broadcast record（是否为广播记录）
     */
    void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException;

    /**
     * Close the accumulator. This will flush all the remaining data and release all the resources.
     *
     * <p>关闭累积器，刷出所有剩余数据并释放资源。
     */
    void close();
}
