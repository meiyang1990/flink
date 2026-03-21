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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava33.com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation for {@link TieredStorageMemoryManager}. This is to request or recycle buffers
 * from {@link LocalBufferPool} for different memory owners, for example, the tiers, the buffer
 * accumulator, etc.
 *
 * <p>Note that the {@link TieredStorageMemorySpec}s of the tiered storages should be ready when
 * setting up the memory manager. Only after the setup process is finished, the tiered storage can
 * request buffers from this manager.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>Tiered Storage 内存管理器实现，为各存储层和缓冲累积器分配和回收缓冲区。
 *
 * <h3>核心工作流程</h3>
 * <pre>
 * 请求缓冲区流程：
 * ┌─────────────────────────────────────────────────────────────────┐
 * │  requestBufferBlocking(owner)                                   │
 * │      │                                                          │
 * │      ▼                                                          │
 * │  ┌───────────────────────┐                                      │
 * │  │ 检查是否需要触发回收  │ ← 根据 numTriggerReclaimBuffersRatio │
 * │  └───────────┬───────────┘                                      │
 * │              ▼                                                  │
 * │  ┌───────────────────────┐    ┌─────────────────┐               │
 * │  │  从 bufferQueue 获取  │───▶│ 有缓冲区则返回  │               │
 * │  └───────────┬───────────┘    └─────────────────┘               │
 * │              ▼ (队列为空)                                        │
 * │  ┌───────────────────────┐                                      │
 * │  │ 从 BufferPool 请求    │ ← requestBufferBlockingFromPool      │
 * │  └───────────┬───────────┘                                      │
 * │              ▼ (Pool 配额已满)                                   │
 * │  ┌───────────────────────┐                                      │
 * │  │ 阻塞等待队列有缓冲区  │ ← requestBufferBlockingFromQueue     │
 * │  │ (定时触发回收检查)    │                                      │
 * │  └───────────────────────┘                                      │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>线程安全设计</h3>
 * <ul>
 *   <li>{@code numRequestedBuffers}: AtomicInteger，Task 线程和 Netty 线程均可访问
 *   <li>{@code numOwnerRequestedBuffers}: ConcurrentHashMap，线程安全的所有者计数
 *   <li>{@code bufferQueue}: LinkedBlockingQueue，线程安全的缓冲区队列
 *   <li>{@code releasedStateLock}: ReadWriteLock，保护 release 状态的并发访问
 * </ul>
 *
 * <h3>回收触发时机</h3>
 * <ul>
 *   <li>缓冲区使用率超过 {@code numTriggerReclaimBuffersRatio} 阈值
 *   <li>阻塞等待超过 {@code MAX_DELAY_TIME_TO_TRIGGER_RECLAIM_BUFFER_MS} 且队列为空
 * </ul>
 */
public class TieredStorageMemoryManagerImpl implements TieredStorageMemoryManager {

    // -------------------------------------------------------------------------
    //  常量配置
    // -------------------------------------------------------------------------

    /** Time to wait for requesting new buffers before triggering buffer reclaiming. */
    // 首次触发回收前的等待时间（毫秒），后续每次检查会指数增长
    private static final int INITIAL_REQUEST_BUFFER_TIMEOUT_FOR_RECLAIMING_MS = 50;

    /** The maximum delay time before triggering buffer reclaiming. */
    // 触发强制回收的最大延迟时间，超过此时间且队列为空则必须回收
    private static final int MAX_DELAY_TIME_TO_TRIGGER_RECLAIM_BUFFER_MS = 1000;

    // -------------------------------------------------------------------------
    //  内存规格与回收配置
    // -------------------------------------------------------------------------

    /** The tiered storage memory specs of each memory user owner. */
    // 各内存所有者（存储层、累积器等）的内存规格配置
    private final Map<Object, TieredStorageMemorySpec> tieredMemorySpecs;

    /** Listeners used to listen the requests for reclaiming buffer in different tiered storage. */
    // 缓冲区回收请求的监听器列表，当需要回收时会通知所有监听器
    private final List<Runnable> bufferReclaimRequestListeners;

    /** The buffer pool usage ratio of triggering the registered storages to reclaim buffers. */
    // 触发回收的缓冲池使用率阈值（0.0 ~ 1.0）
    private final float numTriggerReclaimBuffersRatio;

    /**
     * Indicates whether reclaiming of buffers is supported. If supported, when there's a
     * contention, we may try reclaim buffers from the memory owners.
     */
    // 是否支持缓冲区回收机制，启用时会在内存紧张时触发回收
    private final boolean mayReclaimBuffer;

    // -------------------------------------------------------------------------
    //  缓冲区计数与队列（线程安全）
    // -------------------------------------------------------------------------

    /**
     * The number of requested buffers from {@link BufferPool}. This field can be touched both by
     * the task thread and the netty thread, so it is an atomic type.
     */
    // 已从 BufferPool 请求的缓冲区总数，Task 线程和 Netty 线程共享访问
    private final AtomicInteger numRequestedBuffers;

    /**
     * The number of requested buffers from {@link BufferPool} for each memory owner. This field
     * should be thread-safe because it can be touched both by the task thread and the netty thread.
     */
    // 各所有者持有的缓冲区计数，ConcurrentHashMap 保证线程安全
    private final Map<Object, Integer> numOwnerRequestedBuffers;

    /**
     * The queue that contains all available buffers. This field should be thread-safe because it
     * can be touched both by the task thread and the netty thread.
     */
    // 可用缓冲区队列，回收的缓冲区会放入此队列供后续请求复用
    private final BlockingQueue<MemorySegment> bufferQueue;

    /** The lock guarding concurrency issues during releasing. */
    // 保护 release 状态的读写锁，确保回收操作的并发安全
    private final ReadWriteLock releasedStateLock;

    // -------------------------------------------------------------------------
    //  运行时状态
    // -------------------------------------------------------------------------

    /** The number of buffers that are guaranteed to be reclaimed. */
    // 保证可回收的缓冲区数量（来自 GuaranteedReclaimable 所有者的配额总和）
    private int numGuaranteedReclaimableBuffers;

    /**
     * Time gauge to measure that hard backpressure time. Pre-create it to avoid checkNotNull in
     * hot-path for performance purpose.
     */
    // 硬背压时间计量器，用于监控因缓冲区不足导致的阻塞时间
    private TimerGauge hardBackpressureTimerGauge = new TimerGauge();

    /**
     * This is for triggering buffer reclaiming while blocked on requesting new buffers.
     *
     * <p>Note: This can be null iff buffer reclaiming is not supported.
     */
    // 定时调度器，用于在阻塞等待时定期触发回收检查
    @Nullable private ScheduledExecutorService executor;

    /** The buffer pool where the buffer is requested or recycled. */
    // 底层 BufferPool，所有缓冲区的最终来源
    private BufferPool bufferPool;

    /**
     * Indicates whether the {@link TieredStorageMemoryManagerImpl} is initialized. Before setting
     * up, this field is false.
     *
     * <p>Note that before requesting buffers or getting the maximum allowed buffers, this
     * initialized state should be checked.
     */
    // 初始化标志，只有 setup 完成后才能请求缓冲区
    private boolean isInitialized;

    /**
     * Indicates whether the {@link TieredStorageMemoryManagerImpl} is released.
     *
     * <p>Note that before recycling buffers, this released state should be checked to determine
     * whether to recycle the buffer back to the internal queue or to the buffer pool.
     */
    // 释放标志，决定回收的缓冲区是放入内部队列还是直接归还 BufferPool
    @GuardedBy("readWriteLock")
    private boolean isReleased;

    /**
     * The constructor of the {@link TieredStorageMemoryManagerImpl}.
     *
     * <p>构造函数，初始化回收阈值和各数据结构，但尚未绑定 BufferPool。
     *
     * @param numTriggerReclaimBuffersRatio the buffer pool usage ratio of requesting each tiered
     *     storage to reclaim buffers
     * @param mayReclaimBuffer indicate whether buffer reclaiming is supported
     */
    public TieredStorageMemoryManagerImpl(
            float numTriggerReclaimBuffersRatio, boolean mayReclaimBuffer) {
        this.numTriggerReclaimBuffersRatio = numTriggerReclaimBuffersRatio;
        this.mayReclaimBuffer = mayReclaimBuffer;
        this.tieredMemorySpecs = new HashMap<>();
        this.numRequestedBuffers = new AtomicInteger(0);
        this.numOwnerRequestedBuffers = new ConcurrentHashMap<>();
        this.bufferReclaimRequestListeners = new ArrayList<>();
        this.bufferQueue = new LinkedBlockingQueue<>();
        this.releasedStateLock = new ReentrantReadWriteLock();
        this.isReleased = false;
        this.isInitialized = false;
    }

    /**
     * 初始化内存管理器。
     *
     * <p>核心工作：
     * <ul>
     *   <li>绑定 BufferPool
     *   <li>注册各存储层的内存规格
     *   <li>计算保证可回收缓冲区总数
     *   <li>如支持回收，创建定时检查线程
     * </ul>
     */
    @Override
    public void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs) {
        this.bufferPool = bufferPool;
        // 注册各所有者的内存规格，并累加保证可回收的缓冲区配额
        for (TieredStorageMemorySpec memorySpec : storageMemorySpecs) {
            checkState(
                    !tieredMemorySpecs.containsKey(memorySpec.getOwner()),
                    "Duplicated memory spec.");
            tieredMemorySpecs.put(memorySpec.getOwner(), memorySpec);
            numGuaranteedReclaimableBuffers +=
                    memorySpec.isGuaranteedReclaimable() ? memorySpec.getNumGuaranteedBuffers() : 0;
        }

        // 创建定时调度器用于在阻塞等待时触发回收检查
        if (mayReclaimBuffer) {
            this.executor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ThreadFactoryBuilder()
                                    .setNameFormat("buffer reclaim checker")
                                    .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                    .build());
        }

        this.isInitialized = true;
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metricGroup) {
        this.hardBackpressureTimerGauge =
                checkNotNull(metricGroup.getHardBackPressuredTimePerSecond());
    }

    @Override
    public void listenBufferReclaimRequest(Runnable onBufferReclaimRequest) {
        bufferReclaimRequestListeners.add(onBufferReclaimRequest);
    }

    @Override
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    /**
     * 以阻塞方式请求缓冲区。
     *
     * <p>请求优先级：
     * <ol>
     *   <li>从内部缓冲队列 bufferQueue 获取
     *   <li>从 BufferPool 请求新缓冲区
     *   <li>阻塞等待 bufferQueue 有可用缓冲区
     * </ol>
     */
    @Override
    public BufferBuilder requestBufferBlocking(Object owner) {
        checkIsInitialized();

        // 根据使用率决定是否触发回收
        reclaimBuffersIfNeeded(0);

        // 优先从内部队列获取已回收的缓冲区
        MemorySegment memorySegment = bufferQueue.poll();
        if (memorySegment == null) {
            // 队列为空，尝试从 BufferPool 请求新缓冲区
            memorySegment = requestBufferBlockingFromPool();
        }
        if (memorySegment == null) {
            // Pool 配额已满，阻塞等待队列有可用缓冲区
            memorySegment = checkNotNull(requestBufferBlockingFromQueue());
        }

        // 增加该所有者的缓冲区计数，并创建带回收回调的 BufferBuilder
        incNumRequestedBuffer(owner);
        return new BufferBuilder(
                checkNotNull(memorySegment), segment -> recycleBuffer(owner, segment));
    }

    /**
     * 计算指定所有者可使用的最大不可回收缓冲区数量。
     *
     * <p>计算公式：BufferPool 总量 - 其他所有者已使用或预留的缓冲区数
     */
    @Override
    public int getMaxNonReclaimableBuffers(Object owner) {
        checkIsInitialized();

        // 统计其他所有者已使用或预留的缓冲区数量
        int numBuffersUsedOrReservedForOtherOwners = 0;
        for (Map.Entry<Object, TieredStorageMemorySpec> memorySpecEntry :
                tieredMemorySpecs.entrySet()) {
            Object userOwner = memorySpecEntry.getKey();
            TieredStorageMemorySpec storageMemorySpec = memorySpecEntry.getValue();
            if (!userOwner.equals(owner)) {
                int numGuaranteed = storageMemorySpec.getNumGuaranteedBuffers();
                int numRequested = numOwnerRequestedBuffer(userOwner);
                // 取配额和实际使用量的较大值
                numBuffersUsedOrReservedForOtherOwners += Math.max(numGuaranteed, numRequested);
            }
        }
        // Note that a sudden reduction in the size of the buffer pool may result in non-reclaimable
        // buffer memory occupying the guaranteed buffers of other users. However, this occurrence
        // is limited to the memory tier, which is only utilized when downstream registration is in
        // effect. Furthermore, the buffers within the memory tier can be recycled quickly enough,
        // thereby minimizing the impact on the guaranteed buffers of other tiers.
        return bufferPool.getNumBuffers() - numBuffersUsedOrReservedForOtherOwners;
    }

    /**
     * 预留容量，确保有足够的可回收缓冲区。
     *
     * <p>循环从 BufferPool 请求缓冲区放入 bufferQueue，
     * 直到队列大小 + 可回收所有者已持有量 >= 保证量 + 额外请求量。
     */
    @Override
    public boolean ensureCapacity(int numAdditionalBuffers) {
        checkIsInitialized();

        // 统计保证可回收的所有者当前已持有的缓冲区数
        final int numRequestedByGuaranteedReclaimableOwners =
                tieredMemorySpecs.values().stream()
                        .filter(TieredStorageMemorySpec::isGuaranteedReclaimable)
                        .mapToInt(spec -> numOwnerRequestedBuffer(spec.getOwner()))
                        .sum();

        // 循环请求缓冲区直到满足容量要求
        while (bufferQueue.size() + numRequestedByGuaranteedReclaimableOwners
                < numGuaranteedReclaimableBuffers + numAdditionalBuffers) {
            // 检查是否已达到 BufferPool 配额上限
            if (numRequestedBuffers.get() >= bufferPool.getNumBuffers()) {
                return false;
            }

            MemorySegment memorySegment = requestBufferBlockingFromPool();
            if (memorySegment == null) {
                return false;
            }
            bufferQueue.add(memorySegment);
        }
        return true;
    }

    @Override
    public int numOwnerRequestedBuffer(Object owner) {
        return numOwnerRequestedBuffers.getOrDefault(owner, 0);
    }

    /**
     * 转移缓冲区所有权，更新计数并重新绑定 recycler 回调。
     */
    @Override
    public void transferBufferOwnership(Object oldOwner, Object newOwner, Buffer buffer) {
        checkState(buffer.isBuffer(), "Only buffer supports transfer ownership.");
        decNumRequestedBuffer(oldOwner);
        incNumRequestedBuffer(newOwner);
        // 重新绑定 recycler，使回收时更新正确的所有者计数
        buffer.setRecycler(memorySegment -> recycleBuffer(newOwner, memorySegment));
    }

    /**
     * 释放所有资源。
     *
     * <p>核心步骤：
     * <ol>
     *   <li>设置 isReleased 标志
     *   <li>关闭定时调度器
     *   <li>将 bufferQueue 中所有缓冲区归还 BufferPool
     * </ol>
     */
    @Override
    public void release() {
        // 写锁保护状态切换
        try {
            releasedStateLock.writeLock().lock();
            isReleased = true;
        } finally {
            releasedStateLock.writeLock().unlock();
        }
        // 关闭定时调度器
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                    throw new TimeoutException(
                            "Timeout for shutting down the buffer reclaim checker executor.");
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }
        // 清空内部队列，归还所有缓冲区
        while (!bufferQueue.isEmpty()) {
            MemorySegment segment = bufferQueue.poll();
            bufferPool.recycle(segment);
            numRequestedBuffers.decrementAndGet();
        }
    }

    // -------------------------------------------------------------------------
    //  私有方法：缓冲区请求
    // -------------------------------------------------------------------------

    /**
     * 从 BufferPool 请求缓冲区（带背压计时）。
     *
     * @return 成功返回 MemorySegment，已达配额上限返回 null
     */
    @Nullable
    private MemorySegment requestBufferBlockingFromPool() {
        MemorySegment memorySegment = null;

        // 开始记录硬背压时间
        hardBackpressureTimerGauge.markStart();
        while (numRequestedBuffers.get() < bufferPool.getNumBuffers()) {
            memorySegment = bufferPool.requestMemorySegment();
            if (memorySegment == null) {
                try {
                    // Wait until a buffer is available or timeout before entering the next loop
                    // iteration.
                    // 等待 BufferPool 有可用缓冲区，最多等待 100ms 后重试
                    bufferPool.getAvailableFuture().get(100, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            } else {
                numRequestedBuffers.incrementAndGet();
                break;
            }
        }
        // 结束背压计时
        hardBackpressureTimerGauge.markEnd();

        return memorySegment;
    }

    /**
     * 从内部队列阻塞获取缓冲区，同时启动定时回收检查。
     *
     * @return 获取到的 MemorySegment
     */
    private MemorySegment requestBufferBlockingFromQueue() {
        // 创建 Future 用于跟踪请求是否完成
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        // 启动定时检查，在阻塞等待期间定期触发回收
        scheduleCheckRequestBufferFuture(
                requestBufferFuture, INITIAL_REQUEST_BUFFER_TIMEOUT_FOR_RECLAIMING_MS);

        MemorySegment memorySegment = null;
        try {
            // 阻塞等待队列有可用缓冲区
            memorySegment = bufferQueue.take();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        } finally {
            // 标记请求完成，停止定时检查
            requestBufferFuture.complete(null);
        }

        return memorySegment;
    }

    /**
     * 调度定时检查任务，在阻塞等待期间定期触发回收。
     * 延迟时间采用指数退避策略，避免过于频繁检查。
     */
    private void scheduleCheckRequestBufferFuture(
            CompletableFuture<Void> requestBufferFuture, long delayMs) {
        if (!mayReclaimBuffer || requestBufferFuture.isDone()) {
            return;
        }
        checkNotNull(executor)
                .schedule(
                        // The delay time will be doubled after each check to avoid checking the
                        // future too frequently.
                        // 延迟时间翻倍，指数退避
                        () -> internalCheckRequestBufferFuture(requestBufferFuture, delayMs * 2),
                        delayMs,
                        TimeUnit.MILLISECONDS);
    }

    /**
     * 检查请求是否完成，若未完成则触发回收并重新调度下一次检查。
     */
    private void internalCheckRequestBufferFuture(
            CompletableFuture<Void> requestBufferFuture, long delayForNextCheckMs) {
        if (requestBufferFuture.isDone()) {
            return;
        }
        // 触发回收检查
        reclaimBuffersIfNeeded(delayForNextCheckMs);
        // 调度下一次检查
        scheduleCheckRequestBufferFuture(requestBufferFuture, delayForNextCheckMs);
    }

    // -------------------------------------------------------------------------
    //  私有方法：计数管理
    // -------------------------------------------------------------------------

    /** 增加指定所有者的缓冲区计数 */
    private void incNumRequestedBuffer(Object owner) {
        numOwnerRequestedBuffers.compute(
                owner, (ignore, numRequested) -> numRequested == null ? 1 : numRequested + 1);
    }

    /** 减少指定所有者的缓冲区计数 */
    private void decNumRequestedBuffer(Object owner) {
        numOwnerRequestedBuffers.compute(
                owner, (ignore, numRequested) -> checkNotNull(numRequested) - 1);
    }

    // -------------------------------------------------------------------------
    //  私有方法：回收触发
    // -------------------------------------------------------------------------

    /**
     * 根据条件判断是否需要触发回收，若需要则通知所有监听器。
     */
    private void reclaimBuffersIfNeeded(long delayForNextCheckMs) {
        if (shouldReclaimBuffersBeforeRequesting(delayForNextCheckMs)) {
            bufferReclaimRequestListeners.forEach(Runnable::run);
        }
    }

    /**
     * 判断是否应该触发回收。
     *
     * <p>触发条件（满足任一）：
     * <ul>
     *   <li>缓冲区使用率超过阈值 numTriggerReclaimBuffersRatio
     *   <li>等待时间超过上限且队列为空
     * </ul>
     */
    private boolean shouldReclaimBuffersBeforeRequesting(long delayForNextCheckMs) {
        // The accuracy of the memory usage ratio may be compromised due to the varying buffer pool
        // sizes. However, this only impacts a single iteration of the buffer usage check. Upon the
        // next iteration, the buffer reclaim will eventually be triggered.
        int numTotal = bufferPool.getNumBuffers();
        int numRequested = numRequestedBuffers.get();

        // Because we do the checking before requesting buffers, we need add additional one
        // buffer when calculating the usage ratio.
        // 计算使用率时加 1，因为即将请求一个新缓冲区
        return (numRequested + 1 - bufferQueue.size()) * 1.0 / numTotal
                        > numTriggerReclaimBuffersRatio
                || delayForNextCheckMs > MAX_DELAY_TIME_TO_TRIGGER_RECLAIM_BUFFER_MS
                        && bufferQueue.size() == 0;
    }

    // -------------------------------------------------------------------------
    //  私有方法：缓冲区回收
    // -------------------------------------------------------------------------

    /**
     * 回收缓冲区（可能被 Netty 线程调用）。
     *
     * <p>若未释放且未超出配额，则放入内部队列供复用；否则直接归还 BufferPool。
     */
    /** Note that this method may be called by the netty thread. */
    private void recycleBuffer(Object owner, MemorySegment buffer) {
        try {
            // 读锁保护，允许并发回收
            releasedStateLock.readLock().lock();
            if (!isReleased && numRequestedBuffers.get() <= bufferPool.getNumBuffers()) {
                // 未释放且配额充足，放入内部队列供复用
                bufferQueue.add(buffer);
            } else {
                // 已释放或超出配额，直接归还 BufferPool
                bufferPool.recycle(buffer);
                numRequestedBuffers.decrementAndGet();
            }
        } finally {
            releasedStateLock.readLock().unlock();
        }
        // 减少所有者的持有计数
        decNumRequestedBuffer(owner);
    }

    /** 检查是否已初始化 */
    private void checkIsInitialized() {
        checkState(isInitialized, "The memory manager is not in the running state.");
    }
}
