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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SortMergeResultPartition} appends records and events to {@link DataBuffer} and after the
 * {@link DataBuffer} is full, all data in the {@link DataBuffer} will be copied and spilled to a
 * {@link PartitionedFile} in subpartition index order sequentially. Large records that can not be
 * appended to an empty {@link DataBuffer} will be spilled to the result {@link PartitionedFile}
 * separately.
 *
 * <p>Sort-Merge Shuffle 分区实现。这是 Flink 批处理场景下的核心 Shuffle 实现，相比 Hash Shuffle 具有更好的
 * 磁盘 I/O 效率和内存使用效率。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>排序合并写入</b>：数据先写入内存中的 {@link DataBuffer}，满后按子分区索引顺序排序并溢写到磁盘</li>
 *   <li><b>两种 DataBuffer 策略</b>：根据可用缓冲区数量自动选择 HashBasedDataBuffer 或 SortBasedDataBuffer</li>
 *   <li><b>大记录单独处理</b>：无法放入空 DataBuffer 的大记录会被单独溢写到独立的数据区域</li>
 *   <li><b>随机子分区顺序</b>：写入时使用随机化的子分区顺序，避免所有下游任务以相同顺序读取数据，提升负载均衡</li>
 * </ul>
 *
 * <h2>数据写入流程</h2>
 * <pre>
 * emitRecord/broadcastRecord
 *         ↓
 *    DataBuffer (内存排序缓冲区)
 *         ↓ (满了或需要 flush)
 *    PartitionedFileWriter
 *         ↓
 *    PartitionedFile (磁盘文件: 数据文件 + 索引文件)
 * </pre>
 *
 * <h2>数据读取流程</h2>
 * <pre>
 * createSubpartitionView
 *         ↓
 *    SortMergeResultPartitionReadScheduler (调度读取)
 *         ↓
 *    BatchShuffleReadBufferPool (共享读缓冲池)
 *         ↓
 *    PartitionedFileReader (从磁盘读取)
 * </pre>
 *
 * <h2>与其他组件的关系</h2>
 * <ul>
 *   <li>{@link DataBuffer}：内存中的数据排序缓冲区，有 Hash 和 Sort 两种实现</li>
 *   <li>{@link PartitionedFile}：磁盘上的分区文件，包含数据文件和索引文件</li>
 *   <li>{@link BatchShuffleReadBufferPool}：全局共享的读取缓冲池，多个分区共享</li>
 * </ul>
 *
 * <h2>线程安全说明</h2>
 * <p>该类标记为 @NotThreadSafe，写入操作由单个 Task 线程执行。但 {@link #resultFile} 和
 * {@link #fileWriter} 需要 synchronized 保护，因为读取操作可能来自不同线程。
 *
 * @see DataBuffer
 * @see PartitionedFile
 * @see SortMergeResultPartitionReadScheduler
 */
@NotThreadSafe
public class SortMergeResultPartition extends ResultPartition {

    /**
     * 数据写入时期望分配的缓冲区总字节数。
     * 这是一个经验值 (8MB)，目前不支持配置。较大的值可以减少磁盘 I/O 次数但会占用更多内存。
     */
    private static final int NUM_WRITE_BUFFER_BYTES = 8 * 1024 * 1024;

    /**
     * 批量写入时期望的缓冲区数量。设为 512 意味着每次写入请求最多包含 1024 个缓冲区（包括头部）。
     * 这个值的选择基于 Linux writev 系统调用的限制：单次调用最多写入 1024 个 iovec 结构。
     * 参考 writev(2) man page 了解更多信息。
     */
    private static final int EXPECTED_WRITE_BATCH_SIZE = 512;

    /** 用于保护 resultFile 和 fileWriter 的并发访问的锁对象 */
    private final Object lock = new Object();

    /**
     * 该分区产生的最终结果文件。
     * 包含数据文件和索引文件，在 finish() 方法中由 fileWriter 生成。
     */
    @GuardedBy("lock")
    private PartitionedFile resultFile;

    /** 标记是否已通知用户记录结束事件，避免重复发送 EndOfData 事件 */
    private boolean hasNotifiedEndOfUserRecords;

    /** 网络缓冲区大小，与读取缓冲池的缓冲区大小一致，通常为 32KB */
    private final int networkBufferSize;

    /**
     * 分区文件写入器，负责将 DataBuffer 中的数据按子分区顺序写入磁盘。
     * 写入过程中会维护索引信息，用于后续快速定位各子分区的数据。
     */
    @GuardedBy("lock")
    private PartitionedFileWriter fileWriter;

    /**
     * Shuffle 数据文件和索引文件的存储基础路径。
     * 由 TaskManager 的 shuffle 目录配置决定，支持配置多个目录实现负载均衡。
     */
    private final String resultFileBasePath;

    /**
     * 从 DataBuffer 拷贝数据并写入文件时使用的子分区顺序。
     * 使用随机顺序而非顺序写入，目的是避免所有上游任务的输出被下游任务以相同顺序读取，
     * 从而实现更好的数据输入负载均衡。
     */
    private final int[] subpartitionOrder;

    /**
     * 共享的读取缓冲池，用于读取该分区数据时分配缓冲区。
     * 由 TaskManager 级别管理，多个 SortMergeResultPartition 共享使用。
     */
    private final BatchShuffleReadBufferPool readBufferPool;

    /**
     * 数据读取调度器，负责调度该分区所有子分区的数据读取。
     * 基于优先级队列实现，优先读取磁盘偏移量较小的数据，以优化磁盘 I/O。
     */
    private final SortMergeResultPartitionReadScheduler readScheduler;

    /**
     * 当前可用的网络缓冲区列表，用于一个数据区域的写入。
     * 在 flush 完成后会被回收到 bufferPool，下次写入时重新请求。
     */
    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    /**
     * 用于排序的缓冲区数量。
     * 总可用缓冲区会分配一部分给排序（numBuffersForSort），剩余部分用于批量写入磁盘。
     */
    private int numBuffersForSort;

    /**
     * DataBuffer 实现选择标志。
     * <ul>
     *   <li>true: 使用 HashBasedDataBuffer，当可用缓冲区 >= 2 * numSubpartitions 时</li>
     *   <li>false: 使用 SortBasedDataBuffer，内存占用更少但需要额外的排序开销</li>
     * </ul>
     */
    private boolean useHashBuffer;

    /**
     * 广播数据使用的 DataBuffer。
     * 广播记录会被写入所有子分区，使用独立的 buffer 避免与单播数据混淆。
     */
    private DataBuffer broadcastDataBuffer;

    /**
     * 单播数据使用的 DataBuffer。
     * 普通的 emitRecord 调用会将数据写入指定的目标子分区。
     */
    private DataBuffer unicastDataBuffer;

    public SortMergeResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            BatchShuffleReadBufferPool readBufferPool,
            Executor readIOExecutor,
            ResultPartitionManager partitionManager,
            String resultFileBasePath,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

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

        this.resultFileBasePath = checkNotNull(resultFileBasePath);
        this.readBufferPool = checkNotNull(readBufferPool);
        this.networkBufferSize = readBufferPool.getBufferSize();
        // because IO scheduling will always try to read data in file offset order for better IO
        // performance, when writing data to file, we use a random subpartition order to avoid
        // reading the output of all upstream tasks in the same order, which is better for data
        // input balance of the downstream tasks
        this.subpartitionOrder = getRandomSubpartitionOrder(numSubpartitions);
        this.readScheduler =
                new SortMergeResultPartitionReadScheduler(readBufferPool, readIOExecutor, lock);
    }

    /**
     * 初始化分区，创建文件写入器并预留缓冲区。
     *
     * <p>执行步骤：
     * <ol>
     *   <li>创建 PartitionedFileWriter（索引缓存最大 4MB）</li>
     *   <li>请求"保证"数量的缓冲区，避免死锁</li>
     *   <li>初始化共享读取缓冲池</li>
     * </ol>
     */
    @Override
    protected void setupInternal() throws IOException {
        synchronized (lock) {
            if (isReleased()) {
                throw new IOException("Result partition has been released.");
            }
            try {
                // allocate at most 4M heap memory for caching of index entries
                fileWriter =
                        new PartitionedFileWriter(
                                numSubpartitions, 4194304, resultFileBasePath, subpartitionOrder);
            } catch (Throwable throwable) {
                throw new IOException("Failed to create file writer.", throwable);
            }
        }

        // reserve the "guaranteed" buffers for this buffer pool to avoid the case that those
        // buffers are taken by other result partitions and can not be released, which may cause
        // deadlock
        requestGuaranteedBuffers();

        // initialize the buffer pool eagerly to avoid reporting errors such as OOM too late
        readBufferPool.initialize();
        LOG.info("Sort-merge partition {} initialized.", getPartitionId());
    }

    /**
     * 释放分区资源。
     *
     * <p>分两阶段执行：
     * <ol>
     *   <li>如果结果文件未生成，立即释放 fileWriter</li>
     *   <li>等待所有 reader 释放完成后，删除结果文件</li>
     * </ol>
     */
    @Override
    protected void releaseInternal() {
        synchronized (lock) {
            if (resultFile == null && fileWriter != null) {
                fileWriter.releaseQuietly();
            }
        }

        // delete the produced file only when no reader is reading now
        readScheduler
                .release()
                .thenRun(
                        () -> {
                            synchronized (lock) {
                                if (resultFile != null) {
                                    resultFile.deleteQuietly();
                                    resultFile = null;
                                }
                            }
                        });
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        emit(record, targetSubpartition, DataType.DATA_BUFFER, false);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        broadcast(record, DataType.DATA_BUFFER);
    }

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

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    private void broadcast(ByteBuffer record, DataType dataType) throws IOException {
        emit(record, 0, dataType, true);
    }

    /**
     * 核心数据发送方法，处理单播和广播两种场景。
     *
     * <p>处理流程：
     * <ol>
     *   <li>获取对应的 DataBuffer（广播用 broadcastDataBuffer，单播用 unicastDataBuffer）</li>
     *   <li>尝试将记录追加到 DataBuffer</li>
     *   <li>如果 DataBuffer 已满但记录未写入，说明是大记录，调用 writeLargeRecord 单独处理</li>
     *   <li>如果 DataBuffer 已满且记录已写入，flush DataBuffer 后递归处理剩余数据</li>
     * </ol>
     *
     * @param record 要发送的记录数据
     * @param targetSubpartition 目标子分区索引（广播时为 0）
     * @param dataType 数据类型（DATA_BUFFER 或事件类型）
     * @param isBroadcast 是否为广播记录
     */
    private void emit(
            ByteBuffer record, int targetSubpartition, DataType dataType, boolean isBroadcast)
            throws IOException {
        checkInProduceState();

        DataBuffer dataBuffer = isBroadcast ? getBroadcastDataBuffer() : getUnicastDataBuffer();
        if (!dataBuffer.append(record, targetSubpartition, dataType)) {
            return;
        }

        if (!dataBuffer.hasRemaining()) {
            dataBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
            return;
        }

        flushDataBuffer(dataBuffer, isBroadcast);
        dataBuffer.release();
        if (record.hasRemaining()) {
            emit(record, targetSubpartition, dataType, isBroadcast);
        }
    }

    private void releaseDataBuffer(DataBuffer dataBuffer) {
        if (dataBuffer != null) {
            dataBuffer.release();
        }
    }

    private DataBuffer getUnicastDataBuffer() throws IOException {
        flushBroadcastDataBuffer();

        if (unicastDataBuffer != null
                && !unicastDataBuffer.isFinished()
                && !unicastDataBuffer.isReleased()) {
            return unicastDataBuffer;
        }

        unicastDataBuffer = createNewDataBuffer();
        return unicastDataBuffer;
    }

    private DataBuffer getBroadcastDataBuffer() throws IOException {
        flushUnicastDataBuffer();

        if (broadcastDataBuffer != null
                && !broadcastDataBuffer.isFinished()
                && !broadcastDataBuffer.isReleased()) {
            return broadcastDataBuffer;
        }

        broadcastDataBuffer = createNewDataBuffer();
        return broadcastDataBuffer;
    }

    /**
     * 创建新的 DataBuffer 实例。
     *
     * <p>根据当前可用缓冲区数量自动选择实现：
     * <ul>
     *   <li>缓冲区数量 >= 2 * numSubpartitions: 使用 HashBasedDataBuffer（每个子分区独立缓冲区）</li>
     *   <li>否则: 使用 SortBasedDataBuffer（共享缓冲区 + 排序）</li>
     * </ul>
     */
    private DataBuffer createNewDataBuffer() throws IOException {
        requestNetworkBuffers();

        if (useHashBuffer) {
            return new HashBasedDataBuffer(
                    freeSegments,
                    bufferPool,
                    numSubpartitions,
                    networkBufferSize,
                    numBuffersForSort,
                    subpartitionOrder);
        } else {
            return new SortBasedDataBuffer(
                    freeSegments,
                    bufferPool,
                    numSubpartitions,
                    networkBufferSize,
                    numBuffersForSort,
                    subpartitionOrder);
        }
    }

    private void requestGuaranteedBuffers() throws IOException {
        int numRequiredBuffer = bufferPool.getNumberOfRequiredMemorySegments();
        if (numRequiredBuffer < 2) {
            throw new IOException(
                    String.format(
                            "Too few sort buffers, please increase %s.",
                            NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS));
        }

        try {
            while (freeSegments.size() < numRequiredBuffer) {
                freeSegments.add(checkNotNull(bufferPool.requestMemorySegmentBlocking()));
            }
        } catch (InterruptedException exception) {
            releaseFreeBuffers();
            throw new IOException("Failed to allocate buffers for result partition.", exception);
        }
    }

    /**
     * 请求网络缓冲区并决定 DataBuffer 实现类型。
     *
     * <p>缓冲区分配策略：
     * <ol>
     *   <li>首先保证最小缓冲区数量（避免死锁）</li>
     *   <li>尽可能多地请求缓冲区，但不超过 maxNumberOfMemorySegments</li>
     *   <li>根据可用缓冲区数量选择 Hash 或 Sort 模式</li>
     *   <li>计算用于排序和写入的缓冲区数量分配</li>
     * </ol>
     */
    private void requestNetworkBuffers() throws IOException {
        requestGuaranteedBuffers();

        // avoid taking too many buffers in one result partition
        while (freeSegments.size() < bufferPool.getMaxNumberOfMemorySegments()) {
            MemorySegment segment = bufferPool.requestMemorySegment();
            if (segment == null) {
                break;
            }
            freeSegments.add(segment);
        }

        useHashBuffer = false;
        int numWriteBuffers = 0;
        if (freeSegments.size() >= 2 * numSubpartitions) {
            useHashBuffer = true;
        } else if (networkBufferSize >= NUM_WRITE_BUFFER_BYTES) {
            numWriteBuffers = 1;
        } else {
            numWriteBuffers =
                    Math.min(EXPECTED_WRITE_BATCH_SIZE, NUM_WRITE_BUFFER_BYTES / networkBufferSize);
        }
        numWriteBuffers = Math.min(freeSegments.size() / 2, numWriteBuffers);
        numBuffersForSort = freeSegments.size() - numWriteBuffers;
    }

    /**
     * 将 DataBuffer 中的数据刷写到磁盘文件。
     *
     * <p>执行步骤：
     * <ol>
     *   <li>调用 dataBuffer.finish() 标记数据完成</li>
     *   <li>启动新的数据区域（data region）</li>
     *   <li>批量从 DataBuffer 读取数据，可选压缩后写入文件</li>
     *   <li>释放使用的缓冲区回缓冲池</li>
     * </ol>
     */
    private void flushDataBuffer(DataBuffer dataBuffer, boolean isBroadcast) throws IOException {
        if (dataBuffer == null || dataBuffer.isReleased() || !dataBuffer.hasRemaining()) {
            return;
        }
        dataBuffer.finish();

        Queue<MemorySegment> segments = new ArrayDeque<>(freeSegments);
        int numBuffersToWrite =
                useHashBuffer
                        ? EXPECTED_WRITE_BATCH_SIZE
                        : Math.min(EXPECTED_WRITE_BATCH_SIZE, segments.size());
        List<BufferWithSubpartition> toWrite = new ArrayList<>(numBuffersToWrite);

        fileWriter.startNewRegion(isBroadcast);
        do {
            if (toWrite.size() >= numBuffersToWrite) {
                writeBuffers(toWrite);
                segments = new ArrayDeque<>(freeSegments);
            }

            BufferWithSubpartition bufferWithSubpartition =
                    dataBuffer.getNextBuffer(segments.poll());
            if (bufferWithSubpartition == null) {
                writeBuffers(toWrite);
                break;
            }

            updateStatistics(bufferWithSubpartition, isBroadcast);
            toWrite.add(compressBufferIfPossible(bufferWithSubpartition));
        } while (true);

        releaseFreeBuffers();
    }

    private void flushBroadcastDataBuffer() throws IOException {
        if (broadcastDataBuffer != null) {
            flushDataBuffer(broadcastDataBuffer, true);
            broadcastDataBuffer.release();
            broadcastDataBuffer = null;
        }
    }

    private void flushUnicastDataBuffer() throws IOException {
        if (unicastDataBuffer != null) {
            flushDataBuffer(unicastDataBuffer, false);
            unicastDataBuffer.release();
            unicastDataBuffer = null;
        }
    }

    private BufferWithSubpartition compressBufferIfPossible(
            BufferWithSubpartition bufferWithSubpartition) {
        Buffer buffer = bufferWithSubpartition.getBuffer();
        if (!canBeCompressed(buffer)) {
            return bufferWithSubpartition;
        }

        buffer = checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
        return new BufferWithSubpartition(buffer, bufferWithSubpartition.getSubpartitionIndex());
    }

    private void updateStatistics(
            BufferWithSubpartition bufferWithSubpartition, boolean isBroadcast) {
        numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
        long readableBytes = bufferWithSubpartition.getBuffer().readableBytes();
        if (isBroadcast) {
            resultPartitionBytes.incAll(readableBytes);
        } else {
            resultPartitionBytes.inc(bufferWithSubpartition.getSubpartitionIndex(), readableBytes);
        }
        numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
    }

    /**
     * 将大记录溢写到独立的数据区域。
     *
     * <p>当记录大于 DataBuffer 容量时，会被拆分成多个 networkBufferSize 大小的块，
     * 每个块独立压缩（如果启用）后写入文件。大记录会占用一个独立的 data region。
     *
     * @param record 大记录数据
     * @param targetSubpartition 目标子分区
     * @param dataType 数据类型
     * @param isBroadcast 是否广播
     */
    private void writeLargeRecord(
            ByteBuffer record, int targetSubpartition, DataType dataType, boolean isBroadcast)
            throws IOException {
        // a large record will be spilled to a separated data region
        fileWriter.startNewRegion(isBroadcast);

        List<BufferWithSubpartition> toWrite = new ArrayList<>();
        Queue<MemorySegment> segments = new ArrayDeque<>(freeSegments);

        while (record.hasRemaining()) {
            if (segments.isEmpty()) {
                fileWriter.writeBuffers(toWrite);
                toWrite.clear();
                segments = new ArrayDeque<>(freeSegments);
            }

            int toCopy = Math.min(record.remaining(), networkBufferSize);
            MemorySegment writeBuffer = checkNotNull(segments.poll());
            writeBuffer.put(0, record, toCopy);

            NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);
            BufferWithSubpartition bufferWithSubpartition =
                    new BufferWithSubpartition(buffer, targetSubpartition);
            updateStatistics(bufferWithSubpartition, isBroadcast);
            toWrite.add(compressBufferIfPossible(bufferWithSubpartition));
        }

        fileWriter.writeBuffers(toWrite);
        releaseFreeBuffers();
    }

    private void writeBuffers(List<BufferWithSubpartition> buffers) throws IOException {
        fileWriter.writeBuffers(buffers);
        buffers.forEach(buffer -> buffer.getBuffer().recycleBuffer());
        buffers.clear();
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    /**
     * 完成分区数据写入，生成最终的分区文件。
     *
     * <p>执行步骤：
     * <ol>
     *   <li>广播 EndOfPartitionEvent 事件</li>
     *   <li>刷写剩余的广播数据缓冲区</li>
     *   <li>调用 fileWriter.finish() 生成最终的 PartitionedFile</li>
     * </ol>
     */
    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        checkState(
                unicastDataBuffer == null,
                "The unicast sort buffer should be either null or released.");
        flushBroadcastDataBuffer();

        synchronized (lock) {
            checkState(!isReleased(), "Result partition is already released.");

            resultFile = fileWriter.finish();
            super.finish();
            LOG.info("New partitioned file produced: {}.", resultFile);
        }
    }

    private void releaseFreeBuffers() {
        if (bufferPool != null) {
            freeSegments.forEach(buffer -> bufferPool.recycle(buffer));
            freeSegments.clear();
        }
    }

    @Override
    public void close() {
        releaseFreeBuffers();
        // the close method will always be called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
        super.close();

        IOUtils.closeQuietly(fileWriter);
    }

    @Override
    protected ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        throw new IllegalStateException(
                "This method should not be called for a sort merge result partition.");
    }

    /**
     * 创建子分区视图，用于下游消费数据。
     *
     * <p>与 PipelinedSubpartition 不同，Sort-Merge 分区支持批量读取一组连续的子分区，
     * 这对于 reduce 端的 all-to-all 连接模式可以提高 I/O 效率。
     *
     * @param indexSet 要读取的子分区索引集合
     * @param availabilityListener 数据可用性监听器
     * @return 子分区视图
     */
    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultSubpartitionIndexSet indexSet, BufferAvailabilityListener availabilityListener)
            throws IOException {
        synchronized (lock) {
            checkElementIndex(indexSet.getEndIndex(), numSubpartitions, "Subpartition not found.");
            checkState(!isReleased(), "Partition released.");
            checkState(isFinished(), "Trying to read unfinished blocking partition.");

            if (!resultFile.isReadable()) {
                throw new PartitionNotFoundException(getPartitionId());
            }

            return readScheduler.createSubpartitionReader(
                    availabilityListener, indexSet, resultFile, subpartitionOrder[0]);
        }
    }

    @Override
    public void flushAll() {}

    @Override
    public void flush(int subpartitionIndex) {}

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        return 0;
    }

    /**
     * 生成随机化的子分区顺序数组。
     *
     * <p>使用随机偏移量实现循环移位，确保：
     * <ul>
     *   <li>每个子分区只出现一次</li>
     *   <li>不同 Task 的子分区顺序大概率不同</li>
     *   <li>避免下游任务以相同顺序读取所有上游数据，实现负载均衡</li>
     * </ul>
     *
     * @param numSubpartitions 子分区数量
     * @return 随机化的子分区顺序数组
     */
    private int[] getRandomSubpartitionOrder(int numSubpartitions) {
        int[] order = new int[numSubpartitions];
        Random random = new Random();
        int shift = random.nextInt(numSubpartitions);
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            order[(subpartition + shift) % numSubpartitions] = subpartition;
        }
        return order;
    }

    @VisibleForTesting
    PartitionedFile getResultFile() {
        synchronized (lock) {
            return resultFile;
        }
    }
}
