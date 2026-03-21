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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the {@link
 * NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure deadlock free operation of
 * the network stack by limiting the number of buffers per local buffer pool. It also implements the
 * default mechanism for buffer recycling, which ensures that every buffer is ultimately returned to
 * the network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to match
 * its new size.
 *
 * <p>New buffers can be requested only when {@code numberOfRequestedMemorySegments <
 * currentPoolSize + maxOverdraftBuffersPerGate}. In other words, all buffers exceeding the
 * currentPoolSize will be dynamically regarded as overdraft buffers.
 *
 * <p>Availability is defined as returning a non-overdraft segment on a subsequent {@link
 * #requestBuffer()}/ {@link #requestBufferBuilder()} and heaving a non-blocking {@link
 * #requestBufferBuilderBlocking(int)}. In particular,
 *
 * <ul>
 *   <li>There is at least one {@link #availableMemorySegments}.
 *   <li>No subpartitions has reached {@link #maxBuffersPerChannel}.
 * </ul>
 *
 * <p>To ensure this contract, the implementation eagerly fetches additional memory segments from
 * {@link NetworkBufferPool} as long as it hasn't reached {@link #maxNumberOfMemorySegments} or one
 * subpartition reached the quota.
 *
 * <p>【学习型注释】LocalBufferPool 是两级缓冲池架构中的本地缓冲池，为单个 Task 提供缓冲区管理。
 *
 * <h2>设计目的</h2>
 * <ul>
 *   <li>隔离不同 Task 的缓冲区使用，防止单个 Task 耗尽全局缓冲区</li>
 *   <li>实现细粒度的背压控制（每个 Channel 的缓冲区配额）</li>
 *   <li>支持动态调整缓冲池大小（根据 Task 数量动态分配）</li>
 * </ul>
 *
 * <h2>两级缓冲池架构</h2>
 * <pre>
 *                  NetworkBufferPool（全局）
 *                     /    |    \
 *       LocalBufferPool  LocalBufferPool  LocalBufferPool
 *         (Task1)          (Task2)          (Task3)
 * </pre>
 *
 * <h2>关键参数</h2>
 * <ul>
 *   <li>numberOfRequiredMemorySegments：最小保证缓冲区数（必须满足）</li>
 *   <li>maxNumberOfMemorySegments：最大缓冲区数（弹性上限）</li>
 *   <li>maxBuffersPerChannel：每个 Channel 的缓冲区配额（防止单 Channel 独占）</li>
 *   <li>maxOverdraftBuffersPerGate：允许的透支缓冲区数（应对突发流量）</li>
 * </ul>
 *
 * <h2>可用性定义</h2>
 * 可用性（Availability）表示能否立即获取到非透支的缓冲区，需满足：
 * <ol>
 *   <li>availableMemorySegments 队列非空</li>
 *   <li>没有 Subpartition 达到 maxBuffersPerChannel 上限</li>
 * </ol>
 *
 * <h2>线程安全</h2>
 * 所有操作通过 synchronized(availableMemorySegments) 保护。
 */
public class LocalBufferPool implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

    /** 未知 Channel 标识，表示缓冲区请求不与特定 Channel 关联 */
    private static final int UNKNOWN_CHANNEL = -1;

    /**
     * 全局网络缓冲池的引用，所有本地缓冲池从这里获取底层 MemorySegment。
     */
    /** Global network buffer pool to get buffers from. */
    private final NetworkBufferPool networkBufferPool;

    /**
     * 本地缓冲池的最小保证缓冲区数量。
     * 这个数量必须被满足，否则 Task 无法正常启动。
     */
    /** The minimum number of required segments for this pool. */
    private final int numberOfRequiredMemorySegments;

    /**
     * 当前可用的内存段队列。这些是已从全局缓冲池申请，但尚未被使用的缓冲区。
     *
     * <p>【注意】该对象同时作为同步锁使用，保护所有状态修改操作。
     * 需要特别注意与外部锁的交互，避免死锁（如 BufferManager#bufferQueue）。
     *
     * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
     * locks acquired before entering this class vs. locks being acquired during calls to external
     * code inside this class, e.g. with {@code
     * org.apache.flink.runtime.io.network.partition.consumer.BufferManager#bufferQueue} via the
     * {@link #registeredListeners} callback.
     */
    private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<>();

    /**
     * 缓冲区可用性监听器队列。当缓冲池为空时，消费者可以注册监听器等待缓冲区可用通知。
     * 实现了"推"模式的背压通知机制。
     */
    /**
     * Buffer availability listeners, which need to be notified when a Buffer becomes available.
     * Listeners can only be registered at a time/state where no Buffer instance was available.
     */
    private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

    /**
     * 本地缓冲池能够申请的最大缓冲区数量（弹性上限）。
     */
    /** Maximum number of network buffers to allocate. */
    private final int maxNumberOfMemorySegments;

    /**
     * 当前缓冲池的大小，可在 [numberOfRequiredMemorySegments, maxNumberOfMemorySegments] 范围内动态调整。
     */
    /** The current size of this pool. */
    @GuardedBy("availableMemorySegments")
    private int currentPoolSize;

    /**
     * 已从全局缓冲池申请的缓冲区总数，包括正在使用的和 availableMemorySegments 中的。
     */
    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    @GuardedBy("availableMemorySegments")
    private int numberOfRequestedMemorySegments;

    /**
     * 每个 Channel 的最大缓冲区数量配额。
     * 用于防止单个 Channel 独占缓冲池资源，实现公平的背压控制。
     */
    private final int maxBuffersPerChannel;

    /**
     * 每个 Subpartition 当前使用的缓冲区计数数组。
     * 数组长度等于 Subpartition 数量，索引 i 表示第 i 个 Subpartition 的缓冲区数。
     */
    @GuardedBy("availableMemorySegments")
    private final int[] subpartitionBuffersCount;

    /**
     * 每个 Subpartition 对应的缓冲区回收器。
     * 当 Buffer 被回收时，回收器会更新对应 Subpartition 的计数。
     */
    private final BufferRecycler[] subpartitionBufferRecyclers;

    /**
     * 当前不可用的 Subpartition 数量（已达到 maxBuffersPerChannel 上限）。
     * 当所有 Subpartition 都可用时值为 0。
     */
    @GuardedBy("availableMemorySegments")
    private int unavailableSubpartitionsCount = 0;

    /**
     * 每个 Gate 允许的透支缓冲区数量。
     * 透支缓冲区允许 Task 在突发流量时临时超出 currentPoolSize 限制。
     */
    private int maxOverdraftBuffersPerGate;

    /**
     * 缓冲池是否已销毁的标志。销毁后不能再申请新的缓冲区。
     */
    @GuardedBy("availableMemorySegments")
    private boolean isDestroyed;

    /**
     * 可用性辅助器，管理缓冲池的可用性状态和 CompletableFuture 通知机制。
     */
    @GuardedBy("availableMemorySegments")
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    /**
     * 标记本地缓冲池是否已注册等待全局缓冲池可用的通知。
     * 用于避免重复注册监听器。
     */
    /**
     * Indicates whether this {@link LocalBufferPool} has requested to be notified on the next time
     * that global pool becoming available, so it can then request buffer from the global pool.
     */
    @GuardedBy("availableMemorySegments")
    private boolean requestingNotificationOfGlobalPoolAvailable;

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
     * network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     */
    LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                Integer.MAX_VALUE,
                0,
                Integer.MAX_VALUE,
                0);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
     * number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments,
                0,
                Integer.MAX_VALUE,
                0);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
     * with a minimal and maximal number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     * @param numberOfSubpartitions number of subpartitions
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     * @param maxOverdraftBuffersPerGate maximum number of overdraft buffers to use for each gate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments,
            int numberOfSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate) {
        checkArgument(
                numberOfRequiredMemorySegments > 0,
                "Required number of memory segments (%s) should be larger than 0.",
                numberOfRequiredMemorySegments);

        checkArgument(
                maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
                "Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
                maxNumberOfMemorySegments,
                numberOfRequiredMemorySegments);

        LOG.debug(
                "Using a local buffer pool with {}-{} buffers",
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments);

        this.networkBufferPool = networkBufferPool;
        this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
        this.currentPoolSize = numberOfRequiredMemorySegments;
        this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;

        if (numberOfSubpartitions > 0) {
            checkArgument(
                    maxBuffersPerChannel > 0,
                    "Maximum number of buffers for each channel (%s) should be larger than 0.",
                    maxBuffersPerChannel);
            checkArgument(
                    maxOverdraftBuffersPerGate >= 0,
                    "Maximum number of overdraft buffers for each gate (%s) should not be less than 0.",
                    maxOverdraftBuffersPerGate);
        }

        this.subpartitionBuffersCount = new int[numberOfSubpartitions];
        subpartitionBufferRecyclers = new BufferRecycler[numberOfSubpartitions];
        for (int i = 0; i < subpartitionBufferRecyclers.length; i++) {
            subpartitionBufferRecyclers[i] = new SubpartitionBufferRecycler(i, this);
        }
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;

        // Lock is only taken, because #checkAndUpdateAvailability asserts it. It's a small penalty
        // for thread safety.
        synchronized (this.availableMemorySegments) {
            checkAndUpdateAvailability();
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public void reserveSegments(int numberOfSegmentsToReserve) throws IOException {
        checkArgument(
                numberOfSegmentsToReserve <= numberOfRequiredMemorySegments,
                "Can not reserve more segments than number of required segments.");

        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            checkDestroyed();

            if (numberOfRequestedMemorySegments < numberOfSegmentsToReserve) {
                availableMemorySegments.addAll(
                        networkBufferPool.requestPooledMemorySegmentsBlocking(
                                numberOfSegmentsToReserve - numberOfRequestedMemorySegments));
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    @Override
    public boolean isDestroyed() {
        synchronized (availableMemorySegments) {
            return isDestroyed;
        }
    }

    @Override
    public int getNumberOfRequiredMemorySegments() {
        return numberOfRequiredMemorySegments;
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return maxNumberOfMemorySegments;
    }

    /**
     * Estimates the number of requested buffers.
     *
     * @return the same value as {@link #getMaxNumberOfMemorySegments()} for bounded pools. For
     *     unbounded pools it returns an approximation based upon {@link
     *     #getNumberOfRequiredMemorySegments()}
     */
    public int getEstimatedNumberOfRequestedMemorySegments() {
        if (maxNumberOfMemorySegments < NetworkBufferPool.UNBOUNDED_POOL_SIZE) {
            return maxNumberOfMemorySegments;
        } else {
            return getNumberOfRequiredMemorySegments() * 2;
        }
    }

    @VisibleForTesting
    public int getNumberOfRequestedMemorySegments() {
        synchronized (availableMemorySegments) {
            return numberOfRequestedMemorySegments;
        }
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    @Override
    public int getNumBuffers() {
        synchronized (availableMemorySegments) {
            return currentPoolSize;
        }
    }

    // suppress the FieldAccessNotGuarded warning as this method is unsafe by design.
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
    }

    @Override
    public Buffer requestBuffer() {
        return toBuffer(requestMemorySegment());
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        return toBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(), UNKNOWN_CHANNEL);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        return requestMemorySegmentBlocking(UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
    }

    private Buffer toBuffer(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        return new NetworkBuffer(memorySegment, this);
    }

    private BufferBuilder toBufferBuilder(MemorySegment memorySegment, int targetChannel) {
        if (memorySegment == null) {
            return null;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            return new BufferBuilder(memorySegment, this);
        } else {
            return new BufferBuilder(memorySegment, subpartitionBufferRecyclers[targetChannel]);
        }
    }

    private MemorySegment requestMemorySegmentBlocking(int targetChannel)
            throws InterruptedException {
        MemorySegment segment;
        while ((segment = requestMemorySegment(targetChannel)) == null) {
            try {
                // wait until available
                getAvailableFuture().get();
            } catch (ExecutionException e) {
                LOG.error("The available future is completed exceptionally.", e);
                ExceptionUtils.rethrow(e);
            }
        }
        return segment;
    }

    /**
     * 请求一个内存段用于指定的 Channel。
     *
     * <p>【学习型注释】缓冲区请求流程：
     * <ol>
     *   <li>首先尝试从 availableMemorySegments 获取</li>
     *   <li>如果为空但已达到 currentPoolSize，尝试请求透支缓冲区</li>
     *   <li>更新目标 Channel 的缓冲区计数（如果指定了 Channel）</li>
     *   <li>检查并更新可用性状态</li>
     * </ol>
     */
    @Nullable
    private MemorySegment requestMemorySegment(int targetChannel) {
        MemorySegment segment = null;
        synchronized (availableMemorySegments) {
            checkDestroyed();

            if (!availableMemorySegments.isEmpty()) {
                segment = availableMemorySegments.poll();
            } else if (isRequestedSizeReached()) {
                // Only when the buffer request reaches the upper limit(i.e. current pool size),
                // requests an overdraft buffer.
                segment = requestOverdraftMemorySegmentFromGlobal();
            }

            if (segment == null) {
                return null;
            }

            if (targetChannel != UNKNOWN_CHANNEL) {
                if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
                    unavailableSubpartitionsCount++;
                }
            }

            checkAndUpdateAvailability();
        }
        return segment;
    }

    @GuardedBy("availableMemorySegments")
    private void checkDestroyed() {
        if (isDestroyed) {
            throw new CancelTaskException("Buffer pool has already been destroyed.");
        }
    }

    @Override
    public MemorySegment requestMemorySegment() {
        return requestMemorySegment(UNKNOWN_CHANNEL);
    }

    @GuardedBy("availableMemorySegments")
    private boolean requestMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        if (isRequestedSizeReached()) {
            return false;
        }

        MemorySegment segment = requestPooledMemorySegment();
        if (segment != null) {
            availableMemorySegments.add(segment);
            return true;
        }
        return false;
    }

    @GuardedBy("availableMemorySegments")
    private MemorySegment requestOverdraftMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        // if overdraft buffers(i.e. buffers exceeding poolSize) is greater than or equal to
        // maxOverdraftBuffersPerGate, no new buffer can be requested.
        if (numberOfRequestedMemorySegments - currentPoolSize >= maxOverdraftBuffersPerGate) {
            return null;
        }

        return requestPooledMemorySegment();
    }

    @Nullable
    @GuardedBy("availableMemorySegments")
    private MemorySegment requestPooledMemorySegment() {
        checkState(
                !isDestroyed,
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");

        MemorySegment segment = networkBufferPool.requestPooledMemorySegment();
        if (segment != null) {
            numberOfRequestedMemorySegments++;
        }
        return segment;
    }

    /**
     * Tries to obtain a buffer from global pool as soon as one pool is available. Note that
     * multiple {@link LocalBufferPool}s might wait on the future of the global pool, hence this
     * method double-check if a new buffer is really needed at the time it becomes available.
     */
    @GuardedBy("availableMemorySegments")
    private void requestMemorySegmentFromGlobalWhenAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        checkState(
                !requestingNotificationOfGlobalPoolAvailable,
                "local buffer pool is already in the state of requesting memory segment from global when it is available.");
        requestingNotificationOfGlobalPoolAvailable = true;
        assertNoException(
                networkBufferPool.getAvailableFuture().thenRun(this::onGlobalPoolAvailable));
    }

    private void onGlobalPoolAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (availableMemorySegments) {
            requestingNotificationOfGlobalPoolAvailable = false;
            if (isDestroyed || availabilityHelper.isApproximatelyAvailable()) {
                // there is currently no benefit to obtain buffer from global; give other pools
                // precedent
                return;
            }

            // Check availability and potentially request the memory segment. The call may also
            // result in invoking
            // #requestMemorySegmentFromGlobalWhenAvailable again if no segment could be fetched
            // because of
            // concurrent requests from different LocalBufferPools.
            toNotify = checkAndUpdateAvailability();
        }
        mayNotifyAvailable(toNotify);
    }

    @GuardedBy("availableMemorySegments")
    private boolean shouldBeAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        return !availableMemorySegments.isEmpty() && unavailableSubpartitionsCount == 0;
    }

    @GuardedBy("availableMemorySegments")
    private CompletableFuture<?> checkAndUpdateAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        CompletableFuture<?> toNotify = null;

        AvailabilityStatus availabilityStatus = checkAvailability();
        if (availabilityStatus.isAvailable()) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        } else {
            availabilityHelper.resetUnavailable();
        }
        if (availabilityStatus.isNeedRequestingNotificationOfGlobalPoolAvailable()) {
            requestMemorySegmentFromGlobalWhenAvailable();
        }

        checkConsistentAvailability();
        return toNotify;
    }

    @GuardedBy("availableMemorySegments")
    private AvailabilityStatus checkAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        if (!availableMemorySegments.isEmpty()) {
            return AvailabilityStatus.from(shouldBeAvailable(), false);
        }
        if (isRequestedSizeReached()) {
            return AvailabilityStatus.UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION;
        }
        boolean needRequestingNotificationOfGlobalPoolAvailable = false;
        // There aren't availableMemorySegments, and we continue to request new memory segment from
        // global pool.
        if (!requestMemorySegmentFromGlobal()) {
            // If we can not get a buffer from global pool, we should request from it when it
            // becomes available. It should be noted that if we are already in this status, do not
            // need to repeat the request.
            needRequestingNotificationOfGlobalPoolAvailable =
                    !requestingNotificationOfGlobalPoolAvailable;
        }
        return AvailabilityStatus.from(
                shouldBeAvailable(), needRequestingNotificationOfGlobalPoolAvailable);
    }

    @GuardedBy("availableMemorySegments")
    private void checkConsistentAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        final boolean shouldBeAvailable = shouldBeAvailable();
        checkState(
                availabilityHelper.isApproximatelyAvailable() == shouldBeAvailable,
                "Inconsistent availability: expected " + shouldBeAvailable);
    }

    @Override
    public void recycle(MemorySegment segment) {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    /**
     * 回收缓冲区到缓冲池。
     *
     * <p>【学习型注释】缓冲区回收流程：
     * <ol>
     *   <li>更新对应 Channel 的缓冲区计数</li>
     *   <li>如果缓冲池已销毁或有多余缓冲区，直接归还给全局缓冲池</li>
     *   <li>否则，检查是否有等待的 BufferListener 需要通知</li>
     *   <li>如果有监听器，直接将缓冲区传递给监听器（避免入队再出队）</li>
     *   <li>如果没有监听器，将缓冲区放入 availableMemorySegments 队列</li>
     * </ol>
     */
    private void recycle(MemorySegment segment, int channel) {
        BufferListener listener;
        CompletableFuture<?> toNotify = null;
        do {
            synchronized (availableMemorySegments) {
                if (channel != UNKNOWN_CHANNEL) {
                    if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
                        unavailableSubpartitionsCount--;
                    }
                }

                if (isDestroyed || hasExcessBuffers()) {
                    returnMemorySegment(segment);
                    return;
                } else {
                    listener = registeredListeners.poll();
                    if (listener == null) {
                        availableMemorySegments.add(segment);
                        if (!availabilityHelper.isApproximatelyAvailable() && shouldBeAvailable()) {
                            toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        }
                        break;
                    }
                }

                checkConsistentAvailability();
            }
        } while (!fireBufferAvailableNotification(listener, segment));

        mayNotifyAvailable(toNotify);
    }

    private boolean fireBufferAvailableNotification(
            BufferListener listener, MemorySegment segment) {
        // We do not know which locks have been acquired before the recycle() or are needed in the
        // notification and which other threads also access them.
        // -> call notifyBufferAvailable() outside the synchronized block to avoid a deadlock
        // (FLINK-9676)
        return listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
    }

    /**
     * Destroy is called after the produce or consume phase of a task finishes.
     *
     * <p>【学习型注释】延迟销毁缓冲池。
     * <ul>
     *   <li>将所有可用缓冲区归还给全局缓冲池</li>
     *   <li>通知所有等待的 BufferListener 缓冲池已销毁</li>
     *   <li>标记 isDestroyed = true，后续请求会抛出 CancelTaskException</li>
     *   <li>从 NetworkBufferPool 注销本地缓冲池</li>
     * </ul>
     * "延迟"体现在：已分发出去的缓冲区会在回收时再归还，而非立即强制回收。
     */
    /** Destroy is called after the produce or consume phase of a task finishes. */
    @Override
    public void lazyDestroy() {
        // NOTE: if you change this logic, be sure to update recycle() as well!
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (!isDestroyed) {
                MemorySegment segment;
                while ((segment = availableMemorySegments.poll()) != null) {
                    returnMemorySegment(segment);
                }

                BufferListener listener;
                while ((listener = registeredListeners.poll()) != null) {
                    listener.notifyBufferDestroyed();
                }

                if (!isAvailable()) {
                    toNotify = availabilityHelper.getAvailableFuture();
                }

                isDestroyed = true;
            }
        }

        mayNotifyAvailable(toNotify);

        networkBufferPool.destroyBufferPool(this);
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        synchronized (availableMemorySegments) {
            if (!availableMemorySegments.isEmpty() || isDestroyed) {
                return false;
            }

            registeredListeners.add(listener);
            return true;
        }
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        CompletableFuture<?> toNotify;
        synchronized (availableMemorySegments) {
            checkArgument(
                    numBuffers >= numberOfRequiredMemorySegments,
                    "Buffer pool needs at least %s buffers, but tried to set to %s",
                    numberOfRequiredMemorySegments,
                    numBuffers);

            currentPoolSize = Math.min(numBuffers, maxNumberOfMemorySegments);

            returnExcessMemorySegments();

            if (isDestroyed) {
                // FLINK-19964: when two local buffer pools are released concurrently, one of them
                // gets buffers assigned
                // make sure that checkAndUpdateAvailability is not called as it would proactively
                // acquire one buffer from NetworkBufferPool.
                return;
            }

            toNotify = checkAndUpdateAvailability();
        }

        mayNotifyAvailable(toNotify);
    }

    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
    }

    public int getMaxOverdraftBuffersPerGate() {
        return maxOverdraftBuffersPerGate;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public String toString() {
        synchronized (availableMemorySegments) {
            return String.format(
                    "[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d,"
                            + "subpartitions: %d, maxBuffersPerChannel: %d, destroyed: %s]",
                    currentPoolSize,
                    numberOfRequiredMemorySegments,
                    numberOfRequestedMemorySegments,
                    availableMemorySegments.size(),
                    maxNumberOfMemorySegments,
                    registeredListeners.size(),
                    subpartitionBuffersCount.length,
                    maxBuffersPerChannel,
                    isDestroyed);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Notifies the potential segment consumer of the new available segments by completing the
     * previous uncompleted future.
     */
    private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    @GuardedBy("availableMemorySegments")
    private void returnMemorySegment(MemorySegment segment) {
        assert Thread.holdsLock(availableMemorySegments);

        numberOfRequestedMemorySegments--;
        networkBufferPool.recyclePooledMemorySegment(segment);
    }

    @GuardedBy("availableMemorySegments")
    private void returnExcessMemorySegments() {
        assert Thread.holdsLock(availableMemorySegments);

        while (hasExcessBuffers()) {
            MemorySegment segment = availableMemorySegments.poll();
            if (segment == null) {
                return;
            }

            returnMemorySegment(segment);
        }
    }

    @GuardedBy("availableMemorySegments")
    private boolean hasExcessBuffers() {
        return numberOfRequestedMemorySegments > currentPoolSize;
    }

    @GuardedBy("availableMemorySegments")
    private boolean isRequestedSizeReached() {
        return numberOfRequestedMemorySegments >= currentPoolSize;
    }

    private static class SubpartitionBufferRecycler implements BufferRecycler {

        private final int channel;
        private final LocalBufferPool bufferPool;

        SubpartitionBufferRecycler(int channel, LocalBufferPool bufferPool) {
            this.channel = channel;
            this.bufferPool = bufferPool;
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            bufferPool.recycle(memorySegment, channel);
        }
    }

    /**
     * This class represents the buffer pool's current ground-truth availability and whether to
     * request buffer from global pool when it is available.
     *
     * <p>【学习型注释】缓冲池可用性状态枚举。
     * <ul>
     *   <li>AVAILABLE：缓冲池可用，可以立即获取缓冲区</li>
     *   <li>UNAVAILABLE_NEED_REQUESTING_NOTIFICATION：不可用，需要注册监听全局缓冲池</li>
     *   <li>UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION：不可用，但无需注册监听（已达上限）</li>
     * </ul>
     */
    private enum AvailabilityStatus {
        AVAILABLE(true, false),
        UNAVAILABLE_NEED_REQUESTING_NOTIFICATION(false, true),
        UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION(false, false);

        /** 标识缓冲池当前是否可用 */
        /** Indicates whether the {@link LocalBufferPool} is currently available. */
        private final boolean available;

        /** 标识是否需要注册监听全局缓冲池可用事件 */
        /**
         * Indicates whether to requesting notification of global pool when it becomes available.
         */
        private final boolean needRequestingNotificationOfGlobalPoolAvailable;

        AvailabilityStatus(
                boolean available, boolean needRequestingNotificationOfGlobalPoolAvailable) {
            this.available = available;
            this.needRequestingNotificationOfGlobalPoolAvailable =
                    needRequestingNotificationOfGlobalPoolAvailable;
        }

        public boolean isAvailable() {
            return available;
        }

        public boolean isNeedRequestingNotificationOfGlobalPoolAvailable() {
            return needRequestingNotificationOfGlobalPoolAvailable;
        }

        public static AvailabilityStatus from(
                boolean available, boolean needRequestingNotificationOfGlobalPoolAvailable) {
            if (available) {
                checkState(
                        !needRequestingNotificationOfGlobalPoolAvailable,
                        "available local buffer pool should not request from global.");
                return AVAILABLE;
            } else if (needRequestingNotificationOfGlobalPoolAvailable) {
                return UNAVAILABLE_NEED_REQUESTING_NOTIFICATION;
            } else {
                return UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION;
            }
        }
    }

    @Override
    public int getBuffersCountUnsafe(int targetChannel) {
        return subpartitionBuffersCount[targetChannel];
    }
}
