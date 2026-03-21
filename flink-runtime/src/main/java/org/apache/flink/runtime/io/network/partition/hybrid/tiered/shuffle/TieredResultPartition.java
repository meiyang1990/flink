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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerMetricUpdate;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TieredResultPartition} appends records and events to the tiered storage, which supports
 * the upstream dynamically switches storage tier for writing shuffle data, and the downstream will
 * read data from the relevant tier.
 *
 * <h2>核心设计概述</h2>
 * <p>TieredResultPartition 是 Flink 分层存储（Tiered Storage）Shuffle 机制的核心生产者实现，
 * 支持将 Shuffle 数据动态路由到不同的存储层（内存、磁盘、远程存储等），
 * 下游消费者从相应的存储层读取数据。
 *
 * <h3>1. 分层存储架构</h3>
 * <pre>
 *   +---------------------+
 *   | TieredResultPartition|
 *   +---------------------+
 *            |
 *            v
 *   +---------------------------+
 *   | TieredStorageProducerClient|  <-- 统一的写入入口
 *   +---------------------------+
 *            |
 *   +--------+--------+--------+
 *   |        |        |        |
 *   v        v        v        v
 * +------+ +------+ +------+ +------+
 * | Tier | | Tier | | Tier | | Tier |
 * |Memory| | Disk | |Remote| | ...  |
 * +------+ +------+ +------+ +------+
 *
 *   动态层选择：根据内存压力、数据量等因素动态选择写入哪一层
 * </pre>
 *
 * <h3>2. 核心组件</h3>
 * <ul>
 *   <li><b>TieredStorageProducerClient</b>：统一的写入客户端，负责数据路由和层选择</li>
 *   <li><b>TieredStorageMemoryManager</b>：内存管理器，管理分层存储的内存配额</li>
 *   <li><b>TieredStorageNettyServiceImpl</b>：Netty 服务，为下游创建读取视图</li>
 *   <li><b>TieredStorageResourceRegistry</b>：资源注册表，管理分层存储资源的生命周期</li>
 * </ul>
 *
 * <h3>3. 写入流程</h3>
 * <ol>
 *   <li>emitRecord/broadcastRecord 接收上游数据</li>
 *   <li>调用 TieredStorageProducerClient.write 写入数据</li>
 *   <li>ProducerClient 根据策略选择目标存储层</li>
 *   <li>数据写入选定的存储层</li>
 * </ol>
 *
 * <h3>4. 与传统 Shuffle 的区别</h3>
 * <table border="1">
 *   <tr><th>特性</th><th>TieredResultPartition</th><th>传统 ResultPartition</th></tr>
 *   <tr><td>存储位置</td><td>动态多层（内存/磁盘/远程）</td><td>固定单层</td></tr>
 *   <tr><td>内存管理</td><td>独立的 TieredStorageMemoryManager</td><td>共享 BufferPool</td></tr>
 *   <tr><td>读取方式</td><td>通过 Netty Service 创建视图</td><td>直接创建 SubpartitionView</td></tr>
 *   <tr><td>适用场景</td><td>大规模批处理、异构存储</td><td>通用流/批处理</td></tr>
 * </table>
 *
 * <h3>5. 生命周期</h3>
 * <ul>
 *   <li><b>setupInternal</b>：初始化内存管理器，注册资源释放回调</li>
 *   <li><b>emitRecord/broadcastRecord</b>：写入数据到分层存储</li>
 *   <li><b>finish</b>：广播 EndOfPartitionEvent，关闭 ProducerClient</li>
 *   <li><b>close/releaseInternal</b>：释放内存和分层存储资源</li>
 * </ul>
 *
 * @see TieredStorageProducerClient
 * @see TieredStorageMemoryManager
 * @see TieredStorageNettyServiceImpl
 */
public class TieredResultPartition extends ResultPartition {

    /**
     * 分层存储专用的分区 ID。
     *
     * <p>从 ResultPartitionID 转换而来，用于分层存储内部的资源定位和管理。
     */
    private final TieredStoragePartitionId partitionId;

    /**
     * 分层存储生产者客户端。
     *
     * <p>统一的写入入口，负责：
     * <ul>
     *   <li>接收上游数据</li>
     *   <li>根据策略选择目标存储层</li>
     *   <li>将数据路由到选定的层</li>
     * </ul>
     */
    private final TieredStorageProducerClient tieredStorageProducerClient;

    /**
     * 分层存储资源注册表。
     *
     * <p>管理分层存储资源的生命周期，支持按 partitionId 注册和清理资源。
     */
    private final TieredStorageResourceRegistry tieredStorageResourceRegistry;

    /**
     * 分层存储 Netty 服务。
     *
     * <p>为下游消费者创建 ResultSubpartitionView，提供数据读取能力。
     */
    private final TieredStorageNettyServiceImpl nettyService;

    /**
     * 分层存储内存规格列表。
     *
     * <p>定义各存储层的内存需求和配置，用于初始化 storageMemoryManager。
     */
    private final List<TieredStorageMemorySpec> tieredStorageMemorySpecs;

    /**
     * 分层存储内存管理器。
     *
     * <p>管理分层存储的内存配额，与 BufferPool 协作分配和回收内存。
     */
    private final TieredStorageMemoryManager storageMemoryManager;

    /**
     * 是否已通知用户数据结束。
     *
     * <p>用于确保 EndOfData 事件只广播一次，避免重复通知。
     */
    private boolean hasNotifiedEndOfUserRecords;

    public TieredResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            TieredStorageProducerClient tieredStorageProducerClient,
            TieredStorageResourceRegistry tieredStorageResourceRegistry,
            TieredStorageNettyServiceImpl nettyService,
            List<TieredStorageMemorySpec> tieredStorageMemorySpecs,
            TieredStorageMemoryManager storageMemoryManager) {
        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                numSubpartitions,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.partitionId = TieredStorageIdMappingUtils.convertId(partitionId);
        this.tieredStorageProducerClient = tieredStorageProducerClient;
        this.tieredStorageResourceRegistry = tieredStorageResourceRegistry;
        this.nettyService = nettyService;
        this.tieredStorageMemorySpecs = tieredStorageMemorySpecs;
        this.storageMemoryManager = storageMemoryManager;
    }

    /**
     * 初始化分层存储分区。
     *
     * <p>初始化流程：
     * <ol>
     *   <li>检查分区是否已释放</li>
     *   <li>使用 BufferPool 和内存规格初始化 storageMemoryManager</li>
     *   <li>在资源注册表中注册内存释放回调</li>
     * </ol>
     */
    @Override
    protected void setupInternal() throws IOException {
        if (isReleased()) {
            throw new IOException("Result partition has been released.");
        }
        storageMemoryManager.setup(bufferPool, tieredStorageMemorySpecs);
        tieredStorageResourceRegistry.registerResource(partitionId, storageMemoryManager::release);
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        storageMemoryManager.setMetricGroup(metrics);
        tieredStorageProducerClient.setMetricStatisticsUpdater(
                this::updateProducerMetricStatistics);
    }

    /**
     * 向指定消费者发送记录数据。
     *
     * <p>更新字节统计后，调用 emit 方法将数据写入分层存储。
     */
    @Override
    public void emitRecord(ByteBuffer record, int consumerId) throws IOException {
        resultPartitionBytes.inc(consumerId, record.remaining());
        emit(record, consumerId, Buffer.DataType.DATA_BUFFER, false);
    }

    /**
     * 广播记录到所有消费者。
     *
     * <p>更新所有子分区的字节统计后，调用 broadcast 方法广播数据。
     */
    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        resultPartitionBytes.incAll(record.remaining());
        broadcast(record, Buffer.DataType.DATA_BUFFER);
    }

    /**
     * 广播事件到所有消费者。
     *
     * <p>将事件序列化为 Buffer 后广播，序列化后的 Buffer 会被回收。
     */
    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
        try {
            ByteBuffer serializedEvent = buffer.getNioBufferReadable();
            broadcast(serializedEvent, buffer.getDataType());
        } finally {
            buffer.recycleBuffer();
        }
    }

    /**
     * 广播数据或事件到所有子分区。
     *
     * <p>内部方法，检查生产状态后调用 emit 进行实际写入。
     */
    private void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
        checkInProduceState();
        emit(record, 0, dataType, true);
    }

    /**
     * 将数据写入分层存储。
     *
     * <p>核心写入方法，将数据委托给 TieredStorageProducerClient 处理。
     * ProducerClient 会根据策略选择目标存储层并完成写入。
     *
     * @param record 待写入的数据
     * @param consumerId 目标消费者 ID（广播时忽略）
     * @param dataType 数据类型（记录或事件）
     * @param isBroadcast 是否为广播模式
     */
    private void emit(
            ByteBuffer record, int consumerId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException {
        tieredStorageProducerClient.write(
                record, TieredStorageIdMappingUtils.convertId(consumerId), dataType, isBroadcast);
    }

    /**
     * 更新生产者指标统计。
     *
     * <p>由 TieredStorageProducerClient 回调，更新写入的缓冲区数和字节数。
     */
    private void updateProducerMetricStatistics(
            TieredStorageProducerMetricUpdate metricStatistics) {
        numBuffersOut.inc(metricStatistics.numWriteBuffersDelta());
        numBytesOut.inc(metricStatistics.numWriteBytesDelta());
    }

    /**
     * 为指定子分区创建读取视图。
     *
     * <p>与传统 ResultPartition 不同，这里通过 Netty Service 创建视图，
     * 视图会从分层存储中读取数据。
     */
    @Override
    protected ResultSubpartitionView createSubpartitionView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkState(!isReleased(), "ResultPartition already released.");
        return nettyService.createResultSubpartitionView(
                partitionId, new TieredStorageSubpartitionId(subpartitionId), availabilityListener);
    }

    /**
     * 完成分区写入。
     *
     * <p>完成流程：
     * <ol>
     *   <li>广播 EndOfPartitionEvent 通知下游数据结束</li>
     *   <li>关闭 tieredStorageProducerClient</li>
     *   <li>调用父类 finish 方法</li>
     * </ol>
     */
    @Override
    public void finish() throws IOException {
        checkState(!isReleased(), "Result partition is already released.");
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        tieredStorageProducerClient.close();
        super.finish();
    }

    /**
     * 关闭分区。
     *
     * <p>释放 storageMemoryManager 的内存资源。
     */
    @Override
    public void close() {
        storageMemoryManager.release();
        super.close();
    }

    /**
     * 释放分区内部资源。
     *
     * <p>通过资源注册表清理与该分区关联的所有分层存储资源。
     */
    @Override
    protected void releaseInternal() {
        tieredStorageResourceRegistry.clearResourceFor(partitionId);
    }

    /**
     * 通知用户数据结束。
     *
     * <p>广播 EndOfData 事件，只执行一次。
     */
    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    @Override
    public void flushAll() {
        // Nothing to do.
    }

    @Override
    public void flush(int subpartitionIndex) {
        // Nothing to do.
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        // Nothing to do.
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        // Nothing to do.
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        // Nothing to do.
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        // Nothing to do.
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // Nothing to do.
        return 0;
    }
}
