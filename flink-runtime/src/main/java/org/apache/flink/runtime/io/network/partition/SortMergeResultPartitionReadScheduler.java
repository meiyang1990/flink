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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Data reader for {@link SortMergeResultPartition} which can read data for all downstream tasks
 * consuming the corresponding {@link SortMergeResultPartition}. It always tries to read shuffle
 * data in order of file offset, which maximums the sequential read so can improve the blocking
 * shuffle performance.
 *
 * <p>Sort-Merge Shuffle 的数据读取调度器，负责协调多个下游消费者的读取请求。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>顺序 I/O 优化</b>：使用优先队列按文件偏移量排序 Reader，最大化顺序读取，提升磁盘性能</li>
 *   <li><b>共享文件通道</b>：多个 SubpartitionReader 共享同一对数据/索引文件通道，减少文件描述符占用</li>
 *   <li><b>Buffer 配额控制</b>：通过 BufferPool 限制每个 Partition 的 Buffer 占用，防止内存过度使用</li>
 *   <li><b>异步读取</b>：读取任务在独立的 IO 线程池中执行，不阻塞主线程</li>
 * </ul>
 *
 * <h2>调度流程</h2>
 * <pre>
 * 1. 下游 Task 请求数据 → createSubpartitionReader()
 *    ├── 打开文件通道（首个 Reader）
 *    ├── 创建 PartitionedFileReader
 *    ├── 注册到 allReaders 和 sortedReaders
 *    └── 触发读取调度 mayTriggerReading()
 *
 * 2. 读取调度 run()
 *    ├── allocateBuffers() 从 BufferPool 申请内存
 *    ├── 按文件偏移顺序遍历 sortedReaders
 *    │   └── reader.readBuffers() 读取数据
 *    ├── 处理完成/失败的 Reader
 *    └── 回收未使用的 Buffer
 *
 * 3. Buffer 回收 recycle()
 *    └── 触发新的读取调度 mayTriggerReading()
 * </pre>
 *
 * <h2>死锁预防</h2>
 * <p>当下游 Task 需要按特定顺序消费多个 ResultPartition（如 A 必须在 B 之前完成）时，
 * 如果 B 占用了所有 Buffer，A 无法读取，B 也无法释放 Buffer，造成死锁。
 * 解决方案：通过 {@link #bufferRequestTimeout} 超时检测，超时后 fail 掉所有 Reader，
 * 触发 Task 重启重试。
 *
 * <h2>线程安全</h2>
 * <p>使用 {@link #lock} 保护所有共享状态，包括：
 * <ul>
 *   <li>allReaders / sortedReaders / failedReaders</li>
 *   <li>dataFileChannel / indexFileChannel</li>
 *   <li>isRunning / numRequestedBuffers / isReleased</li>
 * </ul>
 */
class SortMergeResultPartitionReadScheduler implements Runnable, BufferRecycler {

    private static final Logger LOG =
            LoggerFactory.getLogger(SortMergeResultPartitionReadScheduler.class);

    /**
     * Default maximum time (5min) to wait when requesting read buffers from the buffer pool before
     * throwing an exception.
     *
     * <p>Buffer 请求超时时间（默认 5 分钟），用于死锁检测
     */
    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    /** 用于读取 Buffer 头部信息的复用缓冲区 */
    private final ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();

    /** 用于初始化 FileReader 时读取索引的缓冲区 */
    private final ByteBuffer indexEntryBufferInit =
            ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);

    /** 用于读取数据时读取索引的缓冲区 */
    private final ByteBuffer indexEntryBufferRead =
            ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);

    /** 同步锁，保护所有线程不安全的字段 */
    private final Object lock;

    /**
     * A {@link CompletableFuture} to be completed when this read scheduler including all resources
     * is released.
     *
     * <p>释放完成 Future，当所有资源释放完毕后完成，用于外部等待释放
     */
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** 用于分配读取 Buffer 的缓冲池 */
    private final BatchShuffleReadBufferPool bufferPool;

    /** 执行读取任务的 IO 线程池 */
    private final Executor ioExecutor;

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     *
     * <p>Buffer 请求超时时间，超时后会 fail 所有 Reader（死锁保护）
     */
    private final Duration bufferRequestTimeout;

    /** 所有失败待释放的 Reader 集合 */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionReader> failedReaders = new HashSet<>();

    /**
     * All readers waiting to read data of different subpartitions.
     *
     * <p>所有注册的 Reader 集合，包括正在读取和等待读取的
     */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionReader> allReaders = new HashSet<>();

    /**
     * All readers to be read in order. This queue sorts all readers by file offset to achieve
     * better sequential IO.
     *
     * <p>按文件偏移量排序的 Reader 优先队列，实现顺序 I/O 优化
     */
    @GuardedBy("lock")
    private final Queue<SortMergeSubpartitionReader> sortedReaders = new PriorityQueue<>();

    /** 数据文件通道，由所有 Reader 共享 */
    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    /** 索引文件通道，由所有 Reader 共享 */
    @GuardedBy("lock")
    private FileChannel indexFileChannel;

    /**
     * Whether the data reading task is currently running or not. This flag is used when trying to
     * submit the data reading task.
     *
     * <p>读取任务运行状态标记，防止重复提交读取任务
     */
    @GuardedBy("lock")
    private boolean isRunning;

    /**
     * Number of buffers already allocated and still not recycled by this partition reader.
     *
     * <p>当前已分配但未回收的 Buffer 数量，用于流量控制
     */
    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    /** 标记调度器是否已释放 */
    @GuardedBy("lock")
    private volatile boolean isReleased;

    SortMergeResultPartitionReadScheduler(
            BatchShuffleReadBufferPool bufferPool, Executor ioExecutor, Object lock) {
        this(bufferPool, ioExecutor, lock, DEFAULT_BUFFER_REQUEST_TIMEOUT);
    }

    SortMergeResultPartitionReadScheduler(
            BatchShuffleReadBufferPool bufferPool,
            Executor ioExecutor,
            Object lock,
            Duration bufferRequestTimeout) {

        this.lock = checkNotNull(lock);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBufferInit);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBufferRead);
    }

    /**
     * 读取调度主循环。核心逻辑：
     * <ol>
     *   <li>从 BufferPool 申请内存</li>
     *   <li>按文件偏移顺序遍历 Reader，依次读取数据</li>
     *   <li>处理完成和失败的 Reader</li>
     *   <li>回收未使用的 Buffer</li>
     * </ol>
     */
    @Override
    public synchronized void run() {
        Set<SortMergeSubpartitionReader> finishedReaders = new HashSet<>();
        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers();
        } catch (Throwable throwable) {
            // 申请 Buffer 失败，fail 所有 Reader
            LOG.error("Failed to request buffers for data reading.", throwable);
            failSubpartitionReaders(getAllReaders(), throwable);
            removeFinishedAndFailedReaders(0, finishedReaders);
            return;
        }
        checkState(!buffers.isEmpty(), "No buffer available.");
        int numBuffersAllocated = buffers.size();

        // 遍历 Reader 读取数据
        ArrayList<SortMergeSubpartitionReader> unfinishedReaders = new ArrayList<>();
        SortMergeSubpartitionReader subpartitionReader = getNextReader();
        while (subpartitionReader != null) {
            try {
                if (!subpartitionReader.readBuffers(buffers, this)) {
                    // Reader 读取完成
                    finishedReaders.add(subpartitionReader);
                } else {
                    // Reader 未完成，稍后继续
                    unfinishedReaders.add(subpartitionReader);
                }
            } catch (Throwable throwable) {
                // 读取失败，标记为失败
                failSubpartitionReaders(Collections.singletonList(subpartitionReader), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }

            // Buffer 用完，停止本轮读取
            if (buffers.isEmpty()) {
                break;
            }

            // 获取下一个 Reader
            subpartitionReader = getNextReader();
            if (subpartitionReader == null && !unfinishedReaders.isEmpty()) {
                // 所有 Reader 都处理过一轮，将未完成的放回队列继续
                returnUnfinishedReaders(unfinishedReaders);
                subpartitionReader = getNextReader();
            }
        }

        // 回收未使用的 Buffer
        int numBuffersRead = numBuffersAllocated - buffers.size();
        releaseBuffers(buffers);

        // 返回未完成的 Reader，清理完成/失败的 Reader
        returnUnfinishedReaders(unfinishedReaders);
        removeFinishedAndFailedReaders(numBuffersRead, finishedReaders);
    }

    /**
     * 从 BufferPool 申请内存。
     *
     * <p>该方法会循环尝试直到成功获取 Buffer 或超时。超时机制用于防止死锁：
     * 当多个 ResultPartition 需要按特定顺序消费时，如果后消费的 Partition 占用了
     * 所有 Buffer，先消费的 Partition 无法读取，造成死锁。
     *
     * @return 申请到的 Buffer 队列
     * @throws TimeoutException 超时异常，建议增加 batch-shuffle-read.memory 配置
     */
    @VisibleForTesting
    Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            // 检查是否已释放
            // noinspection FieldAccessNotGuarded
            checkState(!isReleased, "Result partition has been already released.");
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));

        // 超时，抛出异常（死锁保护）
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
    }

    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            try {
                bufferPool.recycle(buffers);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    private void failSubpartitionReaders(
            Collection<SortMergeSubpartitionReader> readers, Throwable failureCause) {
        synchronized (lock) {
            failedReaders.addAll(readers);
        }

        for (SortMergeSubpartitionReader reader : readers) {
            try {
                reader.fail(failureCause);
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    private void removeFinishedAndFailedReaders(
            int numBuffersRead, Set<SortMergeSubpartitionReader> finishedReaders) {
        synchronized (lock) {
            for (SortMergeSubpartitionReader reader : finishedReaders) {
                allReaders.remove(reader);
            }
            finishedReaders.clear();

            for (SortMergeSubpartitionReader reader : failedReaders) {
                allReaders.remove(reader);
            }
            failedReaders.clear();

            if (allReaders.isEmpty()) {
                bufferPool.unregisterRequester(this);
                closeFileChannels();
                sortedReaders.clear();
            }

            numRequestedBuffers += numBuffersRead;
            isRunning = false;
            mayTriggerReading();
            mayNotifyReleased();
        }
    }

    @GuardedBy("lock")
    private void mayNotifyReleased() {
        assert Thread.holdsLock(lock);

        if (isReleased && allReaders.isEmpty()) {
            releaseFuture.complete(null);
        }
    }

    private Queue<SortMergeSubpartitionReader> getAllReaders() {
        synchronized (lock) {
            if (isReleased) {
                return new ArrayDeque<>();
            }
            return new ArrayDeque<>(allReaders);
        }
    }

    @Nullable
    private SortMergeSubpartitionReader getNextReader() {
        synchronized (lock) {
            SortMergeSubpartitionReader subpartitionReader = sortedReaders.poll();
            while (subpartitionReader != null && failedReaders.contains(subpartitionReader)) {
                subpartitionReader = sortedReaders.poll();
            }
            return subpartitionReader;
        }
    }

    /** 将未完成的 Reader 放回排序队列 */
    private void returnUnfinishedReaders(ArrayList<SortMergeSubpartitionReader> readers) {
        if (readers != null && !readers.isEmpty()) {
            synchronized (lock) {
                sortedReaders.addAll(readers);
                readers.clear();
            }
        }
    }

    /**
     * 创建子分区读取器。
     *
     * <p>核心流程：
     * <ol>
     *   <li>如果是第一个 Reader，打开文件通道</li>
     *   <li>创建 PartitionedFileReader 并初始化 Region 索引</li>
     *   <li>注册到 allReaders 和 sortedReaders</li>
     *   <li>触发读取调度</li>
     * </ol>
     *
     * @param availabilityListener 数据可用性监听器，用于通知下游有数据可读
     * @param indexSet 要读取的子分区索引集合
     * @param resultFile 分区文件
     * @param subpartitionOrderRotationIndex 子分区写入顺序旋转索引
     */
    SortMergeSubpartitionReader createSubpartitionReader(
            BufferAvailabilityListener availabilityListener,
            ResultSubpartitionIndexSet indexSet,
            PartitionedFile resultFile,
            int subpartitionOrderRotationIndex)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "Partition is already released.");
            // 创建文件读取器
            PartitionedFileReader fileReader =
                    createFileReader(resultFile, indexSet, subpartitionOrderRotationIndex);
            SortMergeSubpartitionReader subpartitionReader =
                    new SortMergeSubpartitionReader(
                            bufferPool.getBufferSize(), availabilityListener, fileReader);
            // 首个 Reader 注册到 BufferPool
            if (allReaders.isEmpty()) {
                bufferPool.registerRequester(this);
            }
            // 注册 Reader
            allReaders.add(subpartitionReader);
            sortedReaders.add(subpartitionReader);
            // 监听 Reader 释放，以便清理
            subpartitionReader
                    .getReleaseFuture()
                    .thenRun(() -> releaseSubpartitionReader(subpartitionReader));

            // 触发读取调度
            mayTriggerReading();
            return subpartitionReader;
        }
    }

    private void releaseSubpartitionReader(SortMergeSubpartitionReader subpartitionReader) {
        synchronized (lock) {
            if (allReaders.contains(subpartitionReader)) {
                failedReaders.add(subpartitionReader);
            }
        }
    }

    @GuardedBy("lock")
    private PartitionedFileReader createFileReader(
            PartitionedFile resultFile,
            ResultSubpartitionIndexSet indexSet,
            int subpartitionOrderRotationIndex)
            throws IOException {
        assert Thread.holdsLock(lock);

        try {
            if (allReaders.isEmpty()) {
                openFileChannels(resultFile);
            }
            PartitionedFileReader partitionedFileReader =
                    new PartitionedFileReader(
                            resultFile,
                            indexSet,
                            dataFileChannel,
                            indexFileChannel,
                            headerBuf,
                            indexEntryBufferRead,
                            subpartitionOrderRotationIndex);
            partitionedFileReader.initRegionIndex(indexEntryBufferInit);
            return partitionedFileReader;
        } catch (Throwable throwable) {
            if (allReaders.isEmpty()) {
                closeFileChannels();
            }
            throw throwable;
        }
    }

    @GuardedBy("lock")
    private void openFileChannels(PartitionedFile resultFile) throws IOException {
        assert Thread.holdsLock(lock);

        closeFileChannels();
        dataFileChannel = openFileChannel(resultFile.getDataFilePath());
        indexFileChannel = openFileChannel(resultFile.getIndexFilePath());
    }

    @GuardedBy("lock")
    private void closeFileChannels() {
        assert Thread.holdsLock(lock);

        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
        dataFileChannel = null;
        indexFileChannel = null;
    }

    /**
     * Buffer 回收回调，由 BufferRecycler 接口定义。
     *
     * <p>当读取的数据被下游消费完成后，Buffer 会被回收，触发新的读取调度
     */
    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;

            // Buffer 回收后可能可以触发新的读取
            mayTriggerReading();
        }
    }

    /**
     * 尝试触发读取任务。
     *
     * <p>触发条件：
     * <ul>
     *   <li>当前没有正在运行的读取任务</li>
     *   <li>有等待读取的 Reader</li>
     *   <li>已分配的 Buffer 数量未超过上限</li>
     *   <li>当前 Partition 分配的 Buffer 数量低于平均值</li>
     * </ul>
     *
     * <p>Buffer 配额控制策略：每个 Partition 最多使用 max(16MB, 2 * numReaders) 个 Buffer，
     * 较大的并行度允许使用更多 Buffer
     */
    @GuardedBy("lock")
    private void mayTriggerReading() {
        assert Thread.holdsLock(lock);

        // 计算最大可分配 Buffer 数（经验值：较大并行度允许更多 Buffer）
        int maxRequestedBuffers =
                Math.max(4 * bufferPool.getNumBuffersPerRequest(), 2 * allReaders.size());

        // 检查触发条件
        if (!isRunning
                && !allReaders.isEmpty()
                && numRequestedBuffers + bufferPool.getNumBuffersPerRequest() <= maxRequestedBuffers
                && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
            isRunning = true;
            // 提交读取任务到 IO 线程池
            ioExecutor.execute(
                    () -> {
                        try {
                            run();
                        } catch (Throwable throwable) {
                            // 处理未预期的异常
                            FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                    Thread.currentThread(), throwable);
                        }
                    });
        }
    }

    /**
     * Releases this read scheduler and returns a {@link CompletableFuture} which will be completed
     * when all resources are released.
     *
     * <p>释放调度器并返回完成 Future。释放后所有 Reader 会收到 IllegalStateException
     */
    CompletableFuture<?> release() {
        List<SortMergeSubpartitionReader> pendingReaders;
        synchronized (lock) {
            if (isReleased) {
                return releaseFuture;
            }
            isReleased = true;

            failedReaders.addAll(allReaders);
            pendingReaders = new ArrayList<>(allReaders);
            mayNotifyReleased();
        }

        failSubpartitionReaders(
                pendingReaders,
                new IllegalStateException("Result partition has been already released."));
        return releaseFuture;
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    @VisibleForTesting
    int getNumPendingReaders() {
        synchronized (lock) {
            return allReaders.size();
        }
    }

    @VisibleForTesting
    FileChannel getDataFileChannel() {
        synchronized (lock) {
            return dataFileChannel;
        }
    }

    @VisibleForTesting
    FileChannel getIndexFileChannel() {
        synchronized (lock) {
            return indexFileChannel;
        }
    }

    @VisibleForTesting
    CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    @VisibleForTesting
    boolean isRunning() {
        synchronized (lock) {
            return isRunning;
        }
    }
}
