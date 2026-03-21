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
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Client of the Tiered Storage used by the producer.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>Tiered Storage 生产者客户端，负责接收上游数据并写入分层存储。
 *
 * <h3>写入流程</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                TieredStorageProducerClient                      │
 * │                                                                 │
 * │  write(record, subpartitionId)                                  │
 * │      │                                                          │
 * │      ▼                                                          │
 * │  ┌─────────────────────────────────────────────────────────────┐│
 * │  │ isBroadcast && !isBroadcastOnly ?                           ││
 * │  │   是：复制到所有子分区                                       ││
 * │  │   否：直接发送到指定子分区                                   ││
 * │  └─────────────────────────────────────────────────────────────┘│
 * │      │                                                          │
 * │      ▼                                                          │
 * │  BufferAccumulator.receive()                                    │
 * │      │ 缓冲区写满后回调                                         │
 * │      ▼                                                          │
 * │  writeAccumulatedBuffer(subpartitionId, buffer)                 │
 * │      │                                                          │
 * │      ▼                                                          │
 * │  ┌─────────────────────────────────────────────────────────────┐│
 * │  │ chooseStorageTierToStartSegment()                           ││
 * │  │   按优先级遍历 TierProducerAgent，选择可用的存储层            ││
 * │  │   优先级：Memory Tier > Disk Tier > Remote Tier              ││
 * │  └─────────────────────────────────────────────────────────────┘│
 * │      │                                                          │
 * │      ▼                                                          │
 * │  TierProducerAgent.tryWrite()                                   │
 * │                                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>Segment 概念</h3>
 * <p>数据按 Segment（段）组织，每个子分区维护当前正在写入的 Segment 索引和对应的存储层。
 * 当存储层无法继续写入时（如内存不足），会切换到下一个 Segment 并选择新的存储层。
 *
 * <h3>广播处理</h3>
 * <ul>
 *   <li>isBroadcastOnly 分区：所有数据都是广播，subpartitionId 固定为 0
 *   <li>非 isBroadcastOnly 分区：广播数据需要复制到所有子分区
 * </ul>
 */
public class TieredStorageProducerClient {

    // -------------------------------------------------------------------------
    //  分区配置
    // -------------------------------------------------------------------------

    // 是否为仅广播分区（所有数据都是广播）
    private final boolean isBroadcastOnly;

    // 子分区数量
    private final int numSubpartitions;

    // -------------------------------------------------------------------------
    //  核心组件
    // -------------------------------------------------------------------------

    // 缓冲区累积器，负责将记录累积为完整缓冲区
    private final BufferAccumulator bufferAccumulator;

    // 缓冲区压缩器（可选），用于压缩写出的数据
    private final BufferCompressor bufferCompressor;

    /**
     * Note that the {@link TierProducerAgent}s are sorted by priority, with a lower index
     * indicating a higher priority.
     */
    // 存储层生产者代理列表，按优先级排序（索引小 = 优先级高）
    // 典型顺序：Memory Tier (0) > Disk Tier (1) > Remote Tier (2)
    private final List<TierProducerAgent> tierProducerAgents;

    // -------------------------------------------------------------------------
    //  每子分区状态
    // -------------------------------------------------------------------------

    /** The current writing segment index for each subpartition. */
    // 各子分区当前正在写入的 Segment 索引，初始值为 -1（表示尚未开始）
    private final int[] currentSubpartitionSegmentId;

    /** The current writing storage tier for each subpartition. */
    // 各子分区当前使用的存储层代理，null 表示需要选择新的存储层
    private final TierProducerAgent[] currentSubpartitionTierAgent;

    // -------------------------------------------------------------------------
    //  指标统计
    // -------------------------------------------------------------------------

    /**
     * The metric statistics for producer client. Note that it is necessary to check whether the
     * value is null before used.
     */
    // 指标统计更新回调，用于上报写入的缓冲区数和字节数
    @Nullable private Consumer<TieredStorageProducerMetricUpdate> metricStatisticsUpdater;

    /**
     * 构造函数，初始化生产者客户端。
     *
     * @param numSubpartitions 子分区数量
     * @param isBroadcastOnly 是否为仅广播分区
     * @param bufferAccumulator 缓冲区累积器
     * @param bufferCompressor 缓冲区压缩器（可为 null）
     * @param tierProducerAgents 按优先级排序的存储层代理列表
     */
    public TieredStorageProducerClient(
            int numSubpartitions,
            boolean isBroadcastOnly,
            BufferAccumulator bufferAccumulator,
            @Nullable BufferCompressor bufferCompressor,
            List<TierProducerAgent> tierProducerAgents) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.bufferAccumulator = bufferAccumulator;
        this.bufferCompressor = bufferCompressor;
        this.tierProducerAgents = tierProducerAgents;
        this.currentSubpartitionSegmentId = new int[numSubpartitions];
        this.currentSubpartitionTierAgent = new TierProducerAgent[numSubpartitions];

        // 初始化所有子分区的 Segment 索引为 -1（尚未开始写入）
        Arrays.fill(currentSubpartitionSegmentId, -1);

        // 注册缓冲区刷出回调
        bufferAccumulator.setup(this::writeAccumulatedBuffer);
    }

    /**
     * Write records to the producer client. The {@link BufferAccumulator} will accumulate the
     * records into buffers.
     *
     * <p>Note that isBroadcast indicates whether the record is broadcast, while isBroadcastOnly
     * indicates whether the result partition is broadcast-only. When the result partition is not
     * broadcast-only and the record is a broadcast record, the record will be written to all the
     * subpartitions.
     *
     * <p>写入记录到生产者客户端。
     *
     * <p>广播逻辑：
     * <ul>
     *   <li>isBroadcastOnly 分区：所有数据直接发送到累积器
     *   <li>非 isBroadcastOnly 分区的广播记录：复制到所有子分区
     * </ul>
     *
     * @param record the written record data
     * @param subpartitionId the subpartition identifier
     * @param dataType the data type of the record
     * @param isBroadcast whether the record is a broadcast record
     */
    public void write(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {

        // 非 isBroadcastOnly 分区的广播记录需要复制到所有子分区
        if (isBroadcast && !isBroadcastOnly) {
            int currentPosition = record.position();
            for (int i = 0; i < numSubpartitions; ++i) {
                // As the tiered storage subpartition ID is created only for broadcast records,
                // which are fewer than normal records, the performance impact of generating new
                // TieredStorageSubpartitionId objects is expected to be manageable. If the
                // performance is significantly affected, this logic will be optimized accordingly.
                bufferAccumulator.receive(
                        record, new TieredStorageSubpartitionId(i), dataType, isBroadcast);
                // 重置 position 以便下一个子分区读取相同数据
                record.position(currentPosition);
            }
        } else {
            // 普通记录或 isBroadcastOnly 分区，直接发送到目标子分区
            bufferAccumulator.receive(record, subpartitionId, dataType, isBroadcast);
        }
    }

    public void setMetricStatisticsUpdater(
            Consumer<TieredStorageProducerMetricUpdate> metricStatisticsUpdater) {
        this.metricStatisticsUpdater = checkNotNull(metricStatisticsUpdater);
    }

    /** 关闭客户端，释放累积器和所有存储层代理 */
    public void close() {
        bufferAccumulator.close();
        tierProducerAgents.forEach(TierProducerAgent::close);
    }

    // -------------------------------------------------------------------------
    //  私有方法：缓冲区写入
    // -------------------------------------------------------------------------

    /**
     * Write the accumulated buffer of this subpartitionId to an appropriate tier. After the tier is
     * decided, the buffer will be written to the selected tier.
     *
     * <p>Note that the method only throws an exception when choosing a storage tier, so the caller
     * should ensure that the buffer is recycled when throwing an exception.
     *
     * <p>将累积的缓冲区写入合适的存储层。
     *
     * <p>写入逻辑：
     * <ol>
     *   <li>若当前子分区无存储层代理，选择一个开始新 Segment
     *   <li>尝试写入当前存储层
     *   <li>若写入失败（如内存不足），切换到新 Segment 并选择新存储层
     * </ol>
     *
     * @param subpartitionId the subpartition identifier
     * @param accumulatedBuffer one accumulated buffer of this subpartition
     * @param numRemainingConsecutiveBuffers 同一 Segment 中剩余待写入的缓冲区数
     */
    private void writeAccumulatedBuffer(
            TieredStorageSubpartitionId subpartitionId,
            Buffer accumulatedBuffer,
            int numRemainingConsecutiveBuffers) {
        int unCompressedSize = accumulatedBuffer.readableBytes();
        try {
            // 若当前子分区尚未绑定存储层，先选择一个
            if (currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()] == null) {
                chooseStorageTierToStartSegment(subpartitionId, numRemainingConsecutiveBuffers + 1);
            }
            // 尝试写入当前存储层
            if (!currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()].tryWrite(
                    subpartitionId,
                    accumulatedBuffer,
                    bufferAccumulator,
                    numRemainingConsecutiveBuffers)) {
                // 写入失败（如内存不足），切换到新 Segment
                chooseStorageTierToStartSegment(subpartitionId, numRemainingConsecutiveBuffers + 1);
                // 新 Segment 的首个缓冲区必须写入成功
                checkState(
                        currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()].tryWrite(
                                subpartitionId,
                                accumulatedBuffer,
                                bufferAccumulator,
                                numRemainingConsecutiveBuffers),
                        "Failed to write the first buffer to the new segment");
            }
        } catch (IOException ioe) {
            // 发生异常时确保缓冲区被回收
            accumulatedBuffer.recycleBuffer();
            ExceptionUtils.rethrow(ioe);
        }
        // 更新写入指标
        updateMetricStatistics(1, unCompressedSize);
    }

    /**
     * 选择存储层并开始新的 Segment。
     *
     * <p>按优先级遍历存储层代理，选择第一个能够开始新 Segment 的存储层。
     * 优先级由 tierProducerAgents 列表顺序决定（索引小 = 优先级高）。
     *
     * @param subpartitionId 子分区 ID
     * @param totalNumBuffers 本 Segment 预计的总缓冲区数
     */
    private void chooseStorageTierToStartSegment(
            TieredStorageSubpartitionId subpartitionId, int totalNumBuffers) throws IOException {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        int segmentIndex = currentSubpartitionSegmentId[subpartitionIndex];
        int nextSegmentIndex = segmentIndex + 1;

        // 按优先级遍历存储层代理
        for (TierProducerAgent tierProducerAgent : tierProducerAgents) {
            if (tierProducerAgent.tryStartNewSegment(
                    subpartitionId, nextSegmentIndex, totalNumBuffers)) {
                // Update the segment index and the chosen storage tier for the subpartition.
                // 更新子分区的 Segment 索引和存储层代理
                currentSubpartitionSegmentId[subpartitionIndex] = nextSegmentIndex;
                currentSubpartitionTierAgent[subpartitionIndex] = tierProducerAgent;
                return;
            }
        }
        // 所有存储层都无法开始新 Segment
        throw new IOException("Failed to choose a storage tier to start a new segment.");
    }

    /** 更新写入指标统计 */
    private void updateMetricStatistics(int numWriteBuffersDelta, int numWriteBytesDelta) {
        checkNotNull(metricStatisticsUpdater)
                .accept(
                        new TieredStorageProducerMetricUpdate(
                                numWriteBuffersDelta, numWriteBytesDelta));
    }
}
