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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 【中文说明】DiskIOScheduler 是磁盘层的 IO 调度器，负责控制从 Shuffle 文件读取数据。
 *
 * <p>核心职责：
 * <ul>
 *   <li>调度和控制磁盘文件读取操作</li>
 *   <li>确保每个子分区的缓冲区按正确顺序读取</li>
 *   <li>管理读取缓冲区的分配和回收</li>
 *   <li>通过 Netty 将读取的数据发送给下游消费者</li>
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 *   ┌────────────────┐    schedule     ┌──────────────────┐    read     ┌────────────────┐
 *   │  IOExecutor    │ ──────────────► │  DiskIOScheduler │ ──────────► │  磁盘文件       │
 *   └────────────────┘                 └──────────────────┘             └────────────────┘
 *                                              │
 *                                              │ write to Netty
 *                                              ▼
 *                                      ┌──────────────────┐
 *                                      │ NettyConnection  │ ──► Consumer
 *                                      │ Writer           │
 *                                      └──────────────────┘
 * </pre>
 *
 * <p>关键设计：
 * <ul>
 *   <li><b>缓冲区配额控制</b>：通过 maxRequestedBuffers 限制单个调度器可分配的最大缓冲区数，
 *       确保 TaskManager 中多个调度器公平共享缓冲池</li>
 *   <li><b>优先级调度</b>：根据文件偏移量对子分区读取器排序，优化顺序读取性能</li>
 *   <li><b>异步调度</b>：使用 ScheduledExecutorService 异步执行读取任务</li>
 *   <li><b>背压感知</b>：当缓冲区不足或消费者处理较慢时，自动降低读取速率</li>
 * </ul>
 *
 * <p>线程安全：通过 synchronized(lock) 保护所有共享状态的访问。
 *
 * <p>实现接口：
 * <ul>
 *   <li>{@link Runnable}：支持被调度执行</li>
 *   <li>{@link BufferRecycler}：支持缓冲区回收</li>
 *   <li>{@link NettyServiceProducer}：支持 Netty 连接建立和断开的回调</li>
 * </ul>
 */
public class DiskIOScheduler implements Runnable, BufferRecycler, NettyServiceProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DiskIOScheduler.class);

    // 用于保护所有共享状态的锁
    private final Object lock = new Object();

    // ==================== 分区和调度配置 ====================

    /** 分区 ID */
    private final TieredStoragePartitionId partitionId;

    /** 负责调度磁盘读取过程的执行器 */
    private final ScheduledExecutorService ioExecutor;

    // ==================== 缓冲区管理 ====================

    /**
     * 专门用于磁盘读取的缓冲池，在 TaskManager 级别共享。
     */
    private final BatchShuffleReadBufferPool bufferPool;

    /**
     * 单个 DiskIOScheduler 可分配且尚未回收的最大缓冲区数量。
     * 这确保了 TaskManager 中不同的 DiskIOScheduler 能够公平地使用缓冲池。
     */
    private final int maxRequestedBuffers;

    /**
     * 从缓冲池请求读取缓冲区的最大等待时间，超时后抛出异常。
     */
    private final Duration bufferRequestTimeout;

    // ==================== 文件读取相关 ====================

    /**
     * 获取 Segment ID 的函数。
     * 当缓冲区索引代表一个 Segment 的第一个缓冲区时，返回对应的 Segment ID。
     * 参数：(子分区 ID, 缓冲区索引) -> Segment ID
     */
    private final BiFunction<Integer, Integer, Integer> segmentIdGetter;

    /** 分区文件读取器，负责实际的磁盘 IO 操作 */
    private final PartitionFileReader partitionFileReader;

    // ==================== 运行时状态（需要同步保护） ====================

    /** 所有已调度的子分区读取器，按连接 ID 索引 */
    @GuardedBy("lock")
    private final Map<NettyConnectionId, ScheduledSubpartitionReader> allScheduledReaders =
            new HashMap<>();

    /** 标识当前是否有读取任务正在运行 */
    @GuardedBy("lock")
    private boolean isRunning;

    /** 当前已请求且尚未回收的缓冲区数量 */
    @GuardedBy("lock")
    private int numRequestedBuffers;

    /** 标识调度器是否已释放 */
    @GuardedBy("lock")
    private boolean isReleased;

    /**
     * 构造函数。
     *
     * @param partitionId 分区 ID
     * @param bufferPool 批量读取缓冲池
     * @param ioExecutor IO 调度执行器
     * @param maxRequestedBuffers 最大可请求缓冲区数
     * @param bufferRequestTimeout 缓冲区请求超时时间
     * @param segmentIdGetter Segment ID 获取函数
     * @param partitionFileReader 分区文件读取器
     */
    public DiskIOScheduler(
            TieredStoragePartitionId partitionId,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            BiFunction<Integer, Integer, Integer> segmentIdGetter,
            PartitionFileReader partitionFileReader) {
        this.partitionId = partitionId;
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        this.segmentIdGetter = segmentIdGetter;
        this.partitionFileReader = partitionFileReader;
        // 向缓冲池注册本调度器为请求者
        bufferPool.registerRequester(this);
    }

    /**
     * 调度器主运行方法：从磁盘文件读取数据到缓冲区。
     *
     * <p>执行流程：
     * <ol>
     *   <li>调用 readBuffersFromFile() 执行实际的文件读取</li>
     *   <li>更新已请求缓冲区计数</li>
     *   <li>根据读取结果决定下一步调度策略</li>
     * </ol>
     */
    @Override
    public synchronized void run() {
        // 执行文件读取，返回实际读取的缓冲区数量
        int numBuffersRead = readBuffersFromFile();
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
            isRunning = false;
        }
        // 根据读取结果决定调度策略
        if (numBuffersRead == 0) {
            // 未读取到数据，延迟 5ms 后重试
            try {
                ioExecutor.schedule(this::triggerScheduling, 5, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                ignoreRejectedExecutionOnShutdown(e);
            }
        } else {
            // 读取到数据，立即触发下一轮调度
            triggerScheduling();
        }
    }

    /**
     * 当 Netty 连接建立时的回调，创建对应的子分区读取器。
     */
    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        synchronized (lock) {
            checkState(!isReleased, "DiskIOScheduler is already released.");
            // 为该连接创建子分区读取器
            ScheduledSubpartitionReader scheduledSubpartitionReader =
                    new ScheduledSubpartitionReader(subpartitionId, nettyConnectionWriter);
            allScheduledReaders.put(
                    nettyConnectionWriter.getNettyConnectionId(), scheduledSubpartitionReader);
            // 触发调度以开始读取数据
            triggerScheduling();
        }
    }

    /**
     * 当 Netty 连接断开时的回调，移除对应的子分区读取器。
     */
    @Override
    public void connectionBroken(NettyConnectionId id) {
        synchronized (lock) {
            allScheduledReaders.remove(id);
        }
    }

    /**
     * 回收缓冲区的回调方法。
     * 将内存段归还给缓冲池，并触发新的调度。
     */
    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;
            // 缓冲区回收后可能有空间继续读取，触发调度
            triggerScheduling();
        }
    }

    /**
     * 释放调度器资源。
     * 清除所有读取器、释放文件读取器、从缓冲池注销。
     */
    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            allScheduledReaders.clear();
            partitionFileReader.release();
            bufferPool.unregisterRequester(this);
        }
    }

    // ------------------------------------------------------------------------
    //  内部方法
    // ------------------------------------------------------------------------

    /**
     * 核心方法：从磁盘文件读取数据到缓冲区。
     *
     * <p>执行流程：
     * <ol>
     *   <li>获取并排序所有待读取的子分区读取器</li>
     *   <li>分配读取缓冲区</li>
     *   <li>按优先级顺序让每个读取器从磁盘加载数据到缓冲区</li>
     *   <li>释放未使用的缓冲区</li>
     * </ol>
     *
     * @return 实际读取的缓冲区数量
     */
    private int readBuffersFromFile() {
        // 获取排序后的读取器列表（按文件偏移量排序以优化顺序读取）
        List<ScheduledSubpartitionReader> scheduledReaders = sortScheduledReaders();
        if (scheduledReaders.isEmpty()) {
            return 0;
        }
        // 分配读取缓冲区
        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers();
        } catch (Exception e) {
            notifyDownstreamSubpartitionFailed(
                    scheduledReaders, e, "Failed to request buffers for data reading.");
            return 0;
        }

        int numBuffersAllocated = buffers.size();
        if (numBuffersAllocated <= 0) {
            return 0;
        }

        // 遍历读取器，依次从磁盘加载数据
        for (ScheduledSubpartitionReader scheduledReader : scheduledReaders) {
            if (buffers.isEmpty()) {
                break;
            }
            try {
                scheduledReader.loadDiskDataToBuffers(buffers, this);
            } catch (IOException e) {
                notifyDownstreamSubpartitionFailed(
                        Collections.singletonList(scheduledReader),
                        e,
                        "Failed to read shuffle data.");
            }
        }
        // 计算实际读取的缓冲区数量并释放未使用的缓冲区
        int numBuffersRead = numBuffersAllocated - buffers.size();
        releaseBuffers(buffers);
        return numBuffersRead;
    }

    /**
     * 获取并排序所有子分区读取器。
     * 按读取优先级（文件偏移量）排序，优化磁盘顺序读取性能。
     */
    private List<ScheduledSubpartitionReader> sortScheduledReaders() {
        List<ScheduledSubpartitionReader> scheduledReaders;
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            scheduledReaders = new ArrayList<>(allScheduledReaders.values());
        }
        // 准备调度：计算每个读取器的优先级
        for (ScheduledSubpartitionReader reader : scheduledReaders) {
            try {
                reader.prepareForScheduling();
            } catch (IOException e) {
                notifyDownstreamSubpartitionFailed(
                        Collections.singletonList(reader), e, "Failed to prepare for scheduling.");
            }
        }
        // 按优先级排序
        Collections.sort(scheduledReaders);
        return scheduledReaders;
    }

    /**
     * 从缓冲池分配读取缓冲区。
     * 如果缓冲池暂时无缓冲区可用，会重试直到超时。
     */
    private Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            synchronized (lock) {
                if (isReleased) {
                    return new ArrayDeque<>();
                }
            }
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));
        // 超时抛出异常，提示用户增加批量读取内存配置
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    /**
     * Send an error response to the downstream to notify the specific subpartition has been failed.
     * The {@link ScheduledSubpartitionReader} responsible for the failed subpartition will also be
     * removed from the {@link DiskIOScheduler}.
     *
     * @param scheduledReaders the readers of the failed subpartitions.
     * @param failureCause the failure cause in the error response.
     * @param errorLog the log printed in the {@link DiskIOScheduler}.
     */
    private void notifyDownstreamSubpartitionFailed(
            List<ScheduledSubpartitionReader> scheduledReaders,
            Throwable failureCause,
            String errorLog) {
        for (ScheduledSubpartitionReader scheduledReader : scheduledReaders) {
            synchronized (lock) {
                allScheduledReaders.remove(scheduledReader.getId());
            }
            scheduledReader.failReader(failureCause);
        }
        LOG.error(errorLog);
    }

    /** 释放未使用的缓冲区，归还给缓冲池 */
    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            bufferPool.recycle(buffers);
            buffers.clear();
        }
    }

    /**
     * 触发调度：根据当前状态决定是否启动新的读取任务。
     *
     * <p>触发条件：
     * <ul>
     *   <li>当前没有正在运行的读取任务</li>
     *   <li>有待读取的子分区</li>
     *   <li>缓冲区配额未用尽</li>
     *   <li>当前请求的缓冲区数量低于平均值</li>
     * </ul>
     */
    private void triggerScheduling() {
        synchronized (lock) {
            // 检查是否满足调度条件
            if (!isRunning
                    && !allScheduledReaders.isEmpty()
                    && numRequestedBuffers + bufferPool.getNumBuffersPerRequest()
                            <= maxRequestedBuffers
                    && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
                isRunning = true;
                try {
                    ioExecutor.execute(
                            () -> {
                                try {
                                    run();
                                } catch (Throwable t) {
                                    LOG.error("Failed to read data.", t);
                                    // ScheduledExecutorService 不会自动处理未捕获异常，需手动处理
                                    FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                            Thread.currentThread(), t);
                                }
                            });
                } catch (RejectedExecutionException e) {
                    ignoreRejectedExecutionOnShutdown(e);
                }
            }
        }
    }

    /** 获取缓冲区请求超时时间点 */
    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
    }

    /** 忽略线程池关闭时的任务拒绝异常 */
    private void ignoreRejectedExecutionOnShutdown(RejectedExecutionException e) {
        LOG.warn(
                "Attempt to submit a task to the shut down batch read thread pool should be ignored. No more tasks should be accepted.",
                e);
    }

    /**
     * 【中文说明】ScheduledSubpartitionReader 负责从磁盘读取单个子分区的数据。
     *
     * <p>核心职责：
     * <ul>
     *   <li>维护子分区的读取状态（当前 Segment ID、缓冲区索引）</li>
     *   <li>从磁盘文件加载数据到缓冲区</li>
     *   <li>将读取的数据通过 Netty 发送给消费者</li>
     *   <li>支持按优先级排序以优化读取顺序</li>
     * </ul>
     *
     * <p>实现 Comparable 接口以支持按文件偏移量排序。
     */
    private class ScheduledSubpartitionReader implements Comparable<ScheduledSubpartitionReader> {

        /** 子分区 ID */
        private final TieredStorageSubpartitionId subpartitionId;

        /** Netty 连接写入器，用于将数据发送给下游消费者 */
        private final NettyConnectionWriter nettyConnectionWriter;

        /** 下一个要读取的 Segment ID，-1 表示需要更新 */
        private int nextSegmentId = -1;

        /** 下一个要读取的缓冲区索引 */
        private int nextBufferIndex;

        /** 读取优先级（基于文件偏移量），用于排序 */
        private long priority;

        /** 标识读取器是否已失败 */
        private boolean isFailed;

        /** 文件读取进度，用于跟踪读取位置 */
        @Nullable private PartitionFileReader.ReadProgress readProgress;

        private ScheduledSubpartitionReader(
                TieredStorageSubpartitionId subpartitionId,
                NettyConnectionWriter nettyConnectionWriter) {
            this.subpartitionId = subpartitionId;
            this.nettyConnectionWriter = nettyConnectionWriter;
        }

        /**
         * 从磁盘加载数据到缓冲区。
         *
         * <p>核心读取循环：
         * <ol>
         *   <li>从缓冲区队列取出一个内存段</li>
         *   <li>调用 partitionFileReader 读取数据</li>
         *   <li>将完整缓冲区写入 Netty，保留部分缓冲区等待下次读取</li>
         * </ol>
         */
        private void loadDiskDataToBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
                throws IOException {

            if (isFailed) {
                throw new IOException(
                        "The scheduled subpartition reader for "
                                + subpartitionId
                                + " has already been failed.");
            }

            CompositeBuffer partialBuffer = null;
            boolean shouldContinueRead = true;
            try {
                // 循环读取直到缓冲区用尽或文件读取完毕
                while (!buffers.isEmpty() && shouldContinueRead && nextSegmentId >= 0) {
                    MemorySegment memorySegment = buffers.poll();
                    PartitionFileReader.ReadBufferResult readBufferResult;
                    try {
                        // 从分区文件读取数据
                        readBufferResult =
                                partitionFileReader.readBuffer(
                                        partitionId,
                                        subpartitionId,
                                        nextSegmentId,
                                        nextBufferIndex,
                                        memorySegment,
                                        recycler,
                                        readProgress,
                                        partialBuffer);
                        if (readBufferResult == null) {
                            // 无数据可读，归还缓冲区
                            buffers.add(memorySegment);
                            break;
                        }
                    } catch (IOException exception) {
                        buffers.add(memorySegment);
                        throw exception;
                    }

                    List<Buffer> readBuffers = readBufferResult.getReadBuffers();
                    shouldContinueRead = readBufferResult.continuousReadSuggested();
                    readProgress = readBufferResult.getReadProgress();
                    if (readBuffers.isEmpty()) {
                        buffers.add(memorySegment);
                        break;
                    }

                    // 写入完整缓冲区，保留部分缓冲区
                    partialBuffer = writeFullBuffersAndGetPartialBuffer(readBuffers);
                }
            } finally {
                // 清理部分缓冲区
                if (partialBuffer != null) {
                    partialBuffer.recycleBuffer();
                }
            }
        }

        /** 实现 Comparable 接口，按优先级排序 */
        @Override
        public int compareTo(ScheduledSubpartitionReader reader) {
            checkArgument(reader != null);
            return Long.compare(getPriority(), reader.getPriority());
        }

        /**
         * 准备调度：更新 Segment ID 并计算读取优先级。
         * 优先级基于文件偏移量，偏移量小的优先读取以优化顺序 IO。
         */
        private void prepareForScheduling() throws IOException {
            if (nextSegmentId < 0) {
                updateSegmentId();
            }
            // 计算优先级：若无有效 Segment 则设为最大值（最低优先级）
            priority =
                    nextSegmentId < 0
                            ? Long.MAX_VALUE
                            : partitionFileReader.getPriority(
                                    partitionId,
                                    subpartitionId,
                                    nextSegmentId,
                                    nextBufferIndex,
                                    readProgress);
        }

        /**
         * 将完整缓冲区写入 Netty，返回部分缓冲区（若有）。
         * 部分缓冲区指跨越多个物理缓冲区的逻辑缓冲区，需等待下次读取补全。
         */
        private CompositeBuffer writeFullBuffersAndGetPartialBuffer(List<Buffer> readBuffers) {
            CompositeBuffer partialBuffer = null;
            for (int i = 0; i < readBuffers.size(); i++) {
                Buffer readBuffer = readBuffers.get(i);
                // 最后一个缓冲区若为部分缓冲区则保留
                if (i == readBuffers.size() - 1 && isPartialBuffer(readBuffer)) {
                    partialBuffer = (CompositeBuffer) readBuffer;
                    continue;
                }
                // 完整缓冲区写入 Netty
                writeNettyBufferAndUpdateSegmentId(readBuffer);
            }
            return partialBuffer;
        }

        /** 判断是否为部分缓冲区（CompositeBuffer 且有缺失长度） */
        private boolean isPartialBuffer(Buffer readBuffer) {
            return readBuffer instanceof CompositeBuffer
                    && ((CompositeBuffer) readBuffer).missingLength() > 0;
        }

        /**
         * 将缓冲区写入 Netty 并更新 Segment ID。
         * 当遇到 END_OF_SEGMENT 标记时，重置 Segment ID 以读取下一个 Segment。
         */
        private void writeNettyBufferAndUpdateSegmentId(Buffer readBuffer) {
            writeToNettyConnectionWriter(
                    NettyPayload.newBuffer(
                            readBuffer, nextBufferIndex++, subpartitionId.getSubpartitionId()));
            // 检查是否到达 Segment 末尾
            if (readBuffer.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
                nextSegmentId = -1;
                updateSegmentId();
            }
        }

        /** 将数据写入 Netty 连接，并在必要时通知数据可用 */
        private void writeToNettyConnectionWriter(NettyPayload nettyPayload) {
            nettyConnectionWriter.writeNettyPayload(nettyPayload);
            // 当队列中数据较少时通知消费者
            if (nettyConnectionWriter.numQueuedPayloads() <= 1
                    || nettyConnectionWriter.numQueuedBufferPayloads() <= 1) {
                notifyAvailable();
            }
        }

        private long getPriority() {
            return priority;
        }

        /** 通知 Netty 连接有数据可用 */
        private void notifyAvailable() {
            nettyConnectionWriter.notifyAvailable();
        }

        /** 标记读取器失败并关闭 Netty 连接 */
        private void failReader(Throwable failureCause) {
            if (isFailed) {
                return;
            }
            isFailed = true;
            nettyConnectionWriter.close(failureCause);
            nettyConnectionWriter.notifyAvailable();
        }

        /** 根据当前缓冲区索引更新 Segment ID */
        private void updateSegmentId() {
            Integer segmentId =
                    segmentIdGetter.apply(subpartitionId.getSubpartitionId(), nextBufferIndex);
            if (segmentId != null) {
                nextSegmentId = segmentId;
                // 发送 Segment 标识消息
                writeToNettyConnectionWriter(NettyPayload.newSegment(segmentId));
            }
        }

        private NettyConnectionId getId() {
            return nettyConnectionWriter.getNettyConnectionId();
        }
    }
}
