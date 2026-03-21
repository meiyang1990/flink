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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fixed-size {@link MemorySegment} pool used by batch shuffle for shuffle data read (currently
 * only used by sort-merge blocking shuffle).
 *
 * <p>批处理 Shuffle 读取专用的固定大小内存缓冲池，目前主要用于 Sort-Merge Blocking Shuffle。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>固定大小</b>：池大小在创建时确定，由配置参数 network.batch-shuffle.read-memory 控制</li>
 *   <li><b>堆外内存</b>：使用 Direct Memory（堆外内存），减少 GC 压力和内存拷贝</li>
 *   <li><b>批量分配</b>：每次请求返回多个 Buffer（默认 4MB 数据量），优化顺序读取性能</li>
 *   <li><b>公平调度</b>：支持多请求者注册，按平均分配原则控制每个请求者的 Buffer 数量</li>
 *   <li><b>延迟初始化</b>：Buffer 在首次请求时才分配，避免不必要的内存占用</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <pre>
 * TaskManager 启动时创建全局 BatchShuffleReadBufferPool
 *       ↓
 * SortMergeResultPartitionReadScheduler 作为请求者注册
 *       ↓
 * 读取数据时调用 requestBuffers() 获取内存段
 *       ↓
 * 数据处理完成后调用 recycle() 归还内存段
 *       ↓
 * Task 结束时注销请求者
 * </pre>
 *
 * <h2>与其他组件的关系</h2>
 * <ul>
 *   <li>{@link SortMergeResultPartitionReadScheduler}：主要使用者，负责调度读取任务</li>
 *   <li>{@link PartitionedFileReader}：实际使用 Buffer 读取文件数据</li>
 *   <li>TaskManager：创建和管理此缓冲池的生命周期</li>
 * </ul>
 *
 * <h2>配置参数</h2>
 * <ul>
 *   <li>{@code taskmanager.network.batch-shuffle-read.memory}：缓冲池总大小</li>
 *   <li>{@code taskmanager.memory.segment-size}：单个 Buffer 大小</li>
 * </ul>
 *
 * <h2>线程安全</h2>
 * <p>所有对 {@link #buffers} 的访问都需要持有 {@code synchronized(buffers)} 锁
 */
public class BatchShuffleReadBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(BatchShuffleReadBufferPool.class);

    /**
     * Memory size in bytes can be allocated from this buffer pool for a single request (4M is for
     * better sequential read).
     *
     * <p>单次请求分配的内存大小（4MB）。较大的请求大小可以减少 I/O 次数，提升顺序读取性能
     */
    public static final int NUM_BYTES_PER_REQUEST = 4 * 1024 * 1024;

    /**
     * Wait for at most 2 seconds before return if there is no enough available buffers currently.
     *
     * <p>Buffer 不足时的最大等待时间（2 秒），避免无限阻塞
     */
    private static final Duration WAITING_TIME = Duration.ofSeconds(2);

    /**
     * Total direct memory size in bytes can can be allocated and used by this buffer pool.
     *
     * <p>缓冲池可使用的总堆外内存大小（字节）
     */
    private final long totalBytes;

    /** 缓冲池中 Buffer 的总数量 */
    private final int numTotalBuffers;

    /** 单个 Buffer 的大小（字节） */
    private final int bufferSize;

    /**
     * The number of buffers to be returned for a single request.
     *
     * <p>单次请求返回的 Buffer 数量，计算公式：min(numTotalBuffers, NUM_BYTES_PER_REQUEST / bufferSize)
     */
    private final int numBuffersPerRequest;

    /**
     * All requesters which need to request buffers from this pool currently.
     *
     * <p>当前注册的所有请求者集合。用于公平分配 Buffer：
     * 每个请求者平均可获得 numTotalBuffers / bufferRequesters.size() 个 Buffer
     */
    private final Set<Object> bufferRequesters = ConcurrentHashMap.newKeySet();

    /**
     * All available buffers in this buffer pool currently.
     *
     * <p>当前可用的 Buffer 队列。所有对此队列的访问都需要同步
     */
    @GuardedBy("buffers")
    private final Queue<MemorySegment> buffers = new ArrayDeque<>();

    /**
     * The timestamp when the last buffer is recycled or allocated.
     *
     * <p>最后一次 Buffer 操作（分配或回收）的时间戳。用于超时检测，
     * 防止死锁（某些请求者长时间持有 Buffer 不释放）
     */
    @GuardedBy("buffers")
    private long lastBufferOperationTimestamp = System.currentTimeMillis();

    /** 标记缓冲池是否已销毁 */
    @GuardedBy("buffers")
    private boolean destroyed;

    /** 标记缓冲池是否已初始化（Buffer 已分配） */
    @GuardedBy("buffers")
    private boolean initialized;

    public BatchShuffleReadBufferPool(long totalBytes, int bufferSize) {
        checkArgument(totalBytes > 0, "Total memory size must be positive.");
        checkArgument(bufferSize > 0, "Size of buffer must be positive.");
        checkArgument(
                totalBytes >= bufferSize,
                String.format(
                        "Illegal configuration, config value for '%s' must be no smaller than '%s',"
                                + " please increase '%s' to at least %d bytes.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key(),
                        TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key(),
                        bufferSize));

        this.totalBytes = totalBytes;
        this.bufferSize = bufferSize;

        this.numTotalBuffers = (int) Math.min(totalBytes / bufferSize, Integer.MAX_VALUE);
        this.numBuffersPerRequest =
                Math.min(numTotalBuffers, Math.max(1, NUM_BYTES_PER_REQUEST / bufferSize));
    }

    @VisibleForTesting
    long getTotalBytes() {
        return totalBytes;
    }

    @VisibleForTesting
    public int getNumTotalBuffers() {
        return numTotalBuffers;
    }

    @VisibleForTesting
    public int getAvailableBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    public int getNumBuffersPerRequest() {
        return numBuffersPerRequest;
    }

    /** 获取单次请求可获取的最大并发请求数（用于流量控制） */
    public int getMaxConcurrentRequests() {
        return numBuffersPerRequest > 0 ? numTotalBuffers / numBuffersPerRequest : 0;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Initializes this buffer pool which allocates all the buffers.
     *
     * <p>初始化缓冲池，分配所有堆外内存 Buffer。
     *
     * <p>注意：此方法是延迟初始化的，在首次 requestBuffers 时自动调用。
     * 如果内存不足，会清理已分配的 Buffer 并抛出 OutOfMemoryError
     */
    public void initialize() {
        synchronized (buffers) {
            checkState(!destroyed, "Buffer pool is already destroyed.");

            if (initialized) {
                return;
            }
            initialized = true;

            try {
                for (int i = 0; i < numTotalBuffers; ++i) {
                    buffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize));
                }
            } catch (OutOfMemoryError outOfMemoryError) {
                int allocated = buffers.size();
                buffers.forEach(MemorySegment::free);
                buffers.clear();
                throw new OutOfMemoryError(
                        String.format(
                                "Can't allocate enough direct buffer for batch shuffle read buffer "
                                        + "pool (bytes allocated: %d, bytes still needed: %d). To "
                                        + "avoid the exception, you need to do one of the following"
                                        + " adjustments: 1) If you have ever decreased %s, you need"
                                        + " to undo the decrement; 2) If you ever increased %s, you"
                                        + " should also increase %s; 3) If neither the above cases,"
                                        + " it usually means some other parts of your application "
                                        + "have consumed too many direct memory and the value of %s"
                                        + " should be increased.",
                                allocated * bufferSize,
                                (numTotalBuffers - allocated) * bufferSize,
                                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                                TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key(),
                                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                                TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key()));
            }
        }

        LOG.info(
                "Batch shuffle IO buffer pool initialized: numBuffers={}, bufferSize={}.",
                numTotalBuffers,
                bufferSize);
    }

    /** 注册 Buffer 请求者，用于公平分配计算 */
    public void registerRequester(Object requester) {
        bufferRequesters.add(requester);
    }

    /** 注销 Buffer 请求者 */
    public void unregisterRequester(Object requester) {
        bufferRequesters.remove(requester);
    }

    /** 获取每个请求者平均可获取的 Buffer 数量 */
    public int getAverageBuffersPerRequester() {
        return Math.max(1, numTotalBuffers / Math.max(1, bufferRequesters.size()));
    }

    /**
     * Requests a collection of buffers (determined by {@link #numBuffersPerRequest}) from this
     * buffer pool.
     *
     * <p>从缓冲池请求一批 Buffer。核心逻辑：
     * <ol>
     *   <li>如果尚未初始化，先进行初始化</li>
     *   <li>等待直到有足够数量的 Buffer 可用（或超时）</li>
     *   <li>批量取出 numBuffersPerRequest 个 Buffer 返回</li>
     * </ol>
     *
     * @return 请求到的 Buffer 列表，如果超时则返回空列表
     */
    public List<MemorySegment> requestBuffers() throws Exception {
        List<MemorySegment> allocated = new ArrayList<>(numBuffersPerRequest);
        synchronized (buffers) {
            checkState(!destroyed, "Buffer pool is already destroyed.");

            // 延迟初始化
            if (!initialized) {
                initialize();
            }

            // 等待足够的 Buffer 可用
            Deadline deadline = Deadline.fromNow(WAITING_TIME);
            while (buffers.size() < numBuffersPerRequest) {
                checkState(!destroyed, "Buffer pool is already destroyed.");

                buffers.wait(WAITING_TIME.toMillis());
                if (!deadline.hasTimeLeft()) {
                    return allocated; // 超时返回空列表
                }
            }

            // 批量取出 Buffer
            while (allocated.size() < numBuffersPerRequest) {
                allocated.add(buffers.poll());
            }
            lastBufferOperationTimestamp = System.currentTimeMillis();
        }
        return allocated;
    }

    /**
     * Recycles the target buffer to this buffer pool. This method should never throw any exception.
     *
     * <p>回收单个 Buffer 到缓冲池
     */
    public void recycle(MemorySegment segment) {
        checkArgument(segment != null, "Buffer must be not null.");

        recycle(Collections.singletonList(segment));
    }

    /**
     * Recycles a collection of buffers to this buffer pool. This method should never throw any
     * exception.
     *
     * <p>批量回收 Buffer 到缓冲池。回收逻辑：
     * <ul>
     *   <li>如果缓冲池已销毁，直接释放 Buffer 内存</li>
     *   <li>否则将 Buffer 放回队列，并在 Buffer 数量达到请求阈值时唤醒等待的请求者</li>
     * </ul>
     */
    public void recycle(Collection<MemorySegment> segments) {
        checkArgument(segments != null, "Buffer list must be not null.");

        if (segments.isEmpty()) {
            return;
        }

        synchronized (buffers) {
            checkState(initialized, "Recycling a buffer before initialization.");

            // 如果缓冲池已销毁，直接释放内存
            if (destroyed) {
                segments.forEach(MemorySegment::free);
                return;
            }

            // 检查是否需要唤醒等待的请求者
            boolean shouldNotify =
                    buffers.size() < numBuffersPerRequest
                            && buffers.size() + segments.size() >= numBuffersPerRequest;
            buffers.addAll(segments);
            lastBufferOperationTimestamp = System.currentTimeMillis();
            if (shouldNotify) {
                buffers.notifyAll();
            }
        }
    }

    /** 获取最后一次 Buffer 操作的时间戳，用于超时检测 */
    public long getLastBufferOperationTimestamp() {
        synchronized (buffers) {
            return lastBufferOperationTimestamp;
        }
    }

    /**
     * Destroys this buffer pool and after which, no buffer can be allocated any more.
     *
     * <p>销毁缓冲池。销毁后：
     * <ul>
     *   <li>清空所有可用 Buffer（不释放内存，等待 GC）</li>
     *   <li>唤醒所有等待的请求者</li>
     *   <li>后续回收的 Buffer 会被直接释放内存</li>
     * </ul>
     */
    public void destroy() {
        synchronized (buffers) {
            destroyed = true;

            buffers.clear();
            buffers.notifyAll();
        }
    }

    public boolean isDestroyed() {
        synchronized (buffers) {
            return destroyed;
        }
    }
}
