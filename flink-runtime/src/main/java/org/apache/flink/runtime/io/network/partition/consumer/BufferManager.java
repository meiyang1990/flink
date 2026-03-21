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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The general buffer manager used by {@link InputChannel} to request/recycle exclusive or floating
 * buffers.
 *
 * <p>InputChannel 使用的通用缓冲区管理器，负责请求和回收独占缓冲区（exclusive）和浮动缓冲区（floating）。
 *
 * <h2>缓冲区类型</h2>
 * <ul>
 *   <li><b>独占缓冲区（Exclusive Buffer）</b>：从全局 {@link MemorySegmentProvider} 分配，
 *       每个 Channel 独享，不参与缓冲池的动态调整，主要用于保证最小吞吐量</li>
 *   <li><b>浮动缓冲区（Floating Buffer）</b>：从本地 {@link BufferPool} 动态请求，
 *       多个 Channel 共享，用于处理流量波动</li>
 * </ul>
 *
 * <h2>Credit-based 流量控制集成</h2>
 * <p>BufferManager 是 Credit-based 流量控制的关键组件：
 * <ul>
 *   <li>通过 numRequiredBuffers 控制需要的缓冲区数量</li>
 *   <li>当缓冲区不足时注册为 BufferListener，等待缓冲区可用</li>
 *   <li>缓冲区数量变化时通知 InputChannel 更新 Credit</li>
 * </ul>
 *
 * <h2>缓冲区优先级</h2>
 * <p>消费时优先使用浮动缓冲区（{@link AvailableBufferQueue#takeBuffer()}），
 * 因为独占缓冲区是保底资源，应该尽量保留。
 *
 * <h2>线程安全说明</h2>
 * <ul>
 *   <li>bufferQueue 对象同时作为锁和数据结构</li>
 *   <li>回收操作可能来自任意线程</li>
 *   <li>为避免死锁，资源回收总是在锁外执行</li>
 * </ul>
 *
 * <h2>典型使用场景</h2>
 * <pre>
 * RemoteInputChannel:
 *   1. requestExclusiveBuffers() - 初始化时分配独占缓冲区
 *   2. requestFloatingBuffers() - 收到 backlog 后请求浮动缓冲区
 *   3. requestBuffer() / requestBufferBlocking() - 获取缓冲区接收数据
 *   4. recycle() - 数据处理完成后回收缓冲区
 * </pre>
 *
 * @see InputChannel
 * @see RemoteInputChannel
 * @see BufferListener
 */
public class BufferManager implements BufferListener, BufferRecycler {

    /**
     * 可用缓冲区队列，封装了独占缓冲区和浮动缓冲区的管理逻辑。
     * 同时作为同步锁使用。
     */
    private final AvailableBufferQueue bufferQueue = new AvailableBufferQueue();

    /**
     * 全局内存段提供者，用于请求和回收独占缓冲区。
     * 通常由 NetworkBufferPool 实现。
     */
    private final MemorySegmentProvider globalPool;

    /** 拥有此 BufferManager 的 InputChannel */
    private final InputChannel inputChannel;

    /**
     * 标记当前是否正在等待浮动缓冲区。
     * 当从 BufferPool 请求缓冲区失败并注册为 listener 时设为 true。
     */
    @GuardedBy("bufferQueue")
    private boolean isWaitingForFloatingBuffers;

    /**
     * 该 InputChannel 需要的缓冲区总数。
     * 由 Credit-based 流量控制根据上游 backlog 动态调整。
     */
    @GuardedBy("bufferQueue")
    private int numRequiredBuffers;

    public BufferManager(
            MemorySegmentProvider globalPool, InputChannel inputChannel, int numRequiredBuffers) {

        this.globalPool = checkNotNull(globalPool);
        this.inputChannel = checkNotNull(inputChannel);
        checkArgument(numRequiredBuffers >= 0);
        this.numRequiredBuffers = numRequiredBuffers;
    }

    // ------------------------------------------------------------------------
    // Buffer request（缓冲区请求）
    // ------------------------------------------------------------------------

    /**
     * 非阻塞请求一个缓冲区。
     *
     * <p>优先返回浮动缓冲区，其次是独占缓冲区。
     * 同时减少 numRequiredBuffers，避免后续分配过多缓冲区。
     *
     * @return 可用缓冲区，如果没有可用则返回 null
     */
    @Nullable
    Buffer requestBuffer() {
        synchronized (bufferQueue) {
            // decrease the number of buffers require to avoid the possibility of
            // allocating more than required buffers after the buffer is taken
            --numRequiredBuffers;
            return bufferQueue.takeBuffer();
        }
    }

    /**
     * 阻塞请求一个缓冲区。
     *
     * <p>如果队列中没有可用缓冲区，会：
     * <ol>
     *   <li>检查 Channel 是否已释放</li>
     *   <li>尝试从 BufferPool 请求浮动缓冲区</li>
     *   <li>如果请求失败，注册为 BufferListener 并等待</li>
     * </ol>
     *
     * @return 请求到的缓冲区
     * @throws InterruptedException 如果等待被中断
     * @throws CancelTaskException 如果 Channel 已释放或 BufferPool 已销毁
     */
    Buffer requestBufferBlocking() throws InterruptedException {
        synchronized (bufferQueue) {
            Buffer buffer;
            while ((buffer = bufferQueue.takeBuffer()) == null) {
                if (inputChannel.isReleased()) {
                    throw new CancelTaskException(
                            "Input channel ["
                                    + inputChannel.channelInfo
                                    + "] has already been released.");
                }
                if (!isWaitingForFloatingBuffers) {
                    BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
                    buffer = bufferPool.requestBuffer();
                    if (buffer == null && shouldContinueRequest(bufferPool)) {
                        continue;
                    }
                }

                if (buffer != null) {
                    return buffer;
                }
                bufferQueue.wait();
            }
            return buffer;
        }
    }

    private boolean shouldContinueRequest(BufferPool bufferPool) {
        if (bufferPool.addBufferListener(this)) {
            isWaitingForFloatingBuffers = true;
            numRequiredBuffers = 1;
            return false;
        } else if (bufferPool.isDestroyed()) {
            throw new CancelTaskException("Local buffer pool has already been released.");
        } else {
            return true;
        }
    }

    /**
     * 从全局缓冲池请求独占缓冲区。
     *
     * <p>独占缓冲区在 Channel 初始化时分配，生命周期与 Channel 相同。
     * 这些缓冲区保证了 Channel 的最小吞吐量，不会被其他 Channel 借用。
     *
     * @param numExclusiveBuffers 需要的独占缓冲区数量
     * @throws IOException 如果分配失败
     */
    void requestExclusiveBuffers(int numExclusiveBuffers) throws IOException {
        checkArgument(numExclusiveBuffers >= 0, "Num exclusive buffers must be non-negative.");
        if (numExclusiveBuffers == 0) {
            return;
        }

        Collection<MemorySegment> segments =
                globalPool.requestUnpooledMemorySegments(numExclusiveBuffers);
        synchronized (bufferQueue) {
            // AvailableBufferQueue::addExclusiveBuffer may release the previously allocated
            // floating buffer, which requires the caller to recycle these released floating
            // buffers. There should be no floating buffers that have been allocated before the
            // exclusive buffers are initialized, so here only a simple assertion is required
            checkState(
                    unsynchronizedGetFloatingBuffersAvailable() == 0,
                    "Bug in buffer allocation logic: floating buffer is allocated before exclusive buffers are initialized.");
            for (MemorySegment segment : segments) {
                bufferQueue.addExclusiveBuffer(
                        new NetworkBuffer(segment, this), numRequiredBuffers);
            }
        }
    }

    /**
     * 根据需求数量请求浮动缓冲区。
     *
     * <p>这是 Credit-based 流量控制的核心方法：
     * <ol>
     *   <li>设置 numRequiredBuffers 为请求数量</li>
     *   <li>尝试从 BufferPool 请求缓冲区</li>
     *   <li>如果 BufferPool 无法满足，注册为 listener 等待通知</li>
     * </ol>
     *
     * @param numRequired 需要的缓冲区数量（通常等于上游 backlog）
     * @return 实际请求到的缓冲区数量
     */
    int requestFloatingBuffers(int numRequired) {
        int numRequestedBuffers = 0;
        synchronized (bufferQueue) {
            // Similar to notifyBufferAvailable(), make sure that we never add a buffer after
            // channel
            // released all buffers via releaseAllResources().
            if (inputChannel.isReleased()) {
                return numRequestedBuffers;
            }

            numRequiredBuffers = numRequired;
            numRequestedBuffers = tryRequestBuffers();
        }
        return numRequestedBuffers;
    }

    private int tryRequestBuffers() {
        assert Thread.holdsLock(bufferQueue);

        int numRequestedBuffers = 0;
        while (bufferQueue.getAvailableBufferSize() < numRequiredBuffers
                && !isWaitingForFloatingBuffers) {
            BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
            Buffer buffer = bufferPool.requestBuffer();
            if (buffer != null) {
                bufferQueue.addFloatingBuffer(buffer);
                numRequestedBuffers++;
            } else if (bufferPool.addBufferListener(this)) {
                isWaitingForFloatingBuffers = true;
                break;
            }
        }
        return numRequestedBuffers;
    }

    // ------------------------------------------------------------------------
    // Buffer recycle（缓冲区回收）
    // ------------------------------------------------------------------------

    /**
     * 回收独占缓冲区。
     *
     * <p>独占缓冲区回收到当前 BufferManager，可能触发浮动缓冲区的释放：
     * 当可用缓冲区数量超过需求时，会释放多余的浮动缓冲区回 BufferPool。
     *
     * <p>这种设计确保了独占缓冲区的优先级，同时避免了资源浪费。
     *
     * @param segment 要回收的独占内存段
     */
    @Override
    public void recycle(MemorySegment segment) {
        @Nullable Buffer releasedFloatingBuffer = null;
        synchronized (bufferQueue) {
            try {
                // Similar to notifyBufferAvailable(), make sure that we never add a buffer
                // after channel released all buffers via releaseAllResources().
                if (inputChannel.isReleased()) {
                    globalPool.recycleUnpooledMemorySegments(Collections.singletonList(segment));
                    return;
                } else {
                    releasedFloatingBuffer =
                            bufferQueue.addExclusiveBuffer(
                                    new NetworkBuffer(segment, this), numRequiredBuffers);
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            } finally {
                bufferQueue.notifyAll();
            }
        }

        if (releasedFloatingBuffer != null) {
            releasedFloatingBuffer.recycleBuffer();
        } else {
            try {
                inputChannel.notifyBufferAvailable(1);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            }
        }
    }

    void releaseFloatingBuffers() {
        Queue<Buffer> buffers;
        synchronized (bufferQueue) {
            numRequiredBuffers = 0;
            buffers = bufferQueue.clearFloatingBuffers();
        }

        // recycle all buffers out of the synchronization block to avoid dead lock
        while (!buffers.isEmpty()) {
            buffers.poll().recycleBuffer();
        }
    }

    /**
     * 释放所有缓冲区（独占和浮动）。
     *
     * <p>释放策略：
     * <ul>
     *   <li>浮动缓冲区：直接回收到 LocalBufferPool</li>
     *   <li>独占缓冲区：收集后批量回收到全局缓冲池，避免触发不必要的缓冲区重分配</li>
     * </ul>
     *
     * @param buffers 要释放的缓冲区队列
     * @throws IOException 如果释放过程中发生错误
     */
    void releaseAllBuffers(ArrayDeque<Buffer> buffers) throws IOException {
        // Gather all exclusive buffers and recycle them to global pool in batch, because
        // we do not want to trigger redistribution of buffers after each recycle.
        final List<MemorySegment> exclusiveRecyclingSegments = new ArrayList<>();

        Exception err = null;
        Buffer buffer;
        while ((buffer = buffers.poll()) != null) {
            try {
                if (buffer.getRecycler() == BufferManager.this) {
                    exclusiveRecyclingSegments.add(buffer.getMemorySegment());
                } else {
                    buffer.recycleBuffer();
                }
            } catch (Exception e) {
                err = firstOrSuppressed(e, err);
            }
        }
        try {
            synchronized (bufferQueue) {
                bufferQueue.releaseAll(exclusiveRecyclingSegments);
                bufferQueue.notifyAll();
            }
        } catch (Exception e) {
            err = firstOrSuppressed(e, err);
        }
        try {
            if (exclusiveRecyclingSegments.size() > 0) {
                globalPool.recycleUnpooledMemorySegments(exclusiveRecyclingSegments);
            }
        } catch (Exception e) {
            err = firstOrSuppressed(e, err);
        }
        if (err != null) {
            throw err instanceof IOException ? (IOException) err : new IOException(err);
        }
    }

    // ------------------------------------------------------------------------
    // Buffer listener notification（缓冲区监听器通知）
    // ------------------------------------------------------------------------

    /**
     * BufferPool 通知有浮动缓冲区可用时的回调。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>检查 Channel 是否已释放（避免死锁）</li>
     *   <li>检查是否仍需要更多缓冲区</li>
     *   <li>将缓冲区加入队列并尝试请求更多</li>
     *   <li>通知 InputChannel 有新缓冲区可用</li>
     * </ol>
     *
     * <p>死锁避免：在锁外检查 isReleased 状态，因为 releaseAllResources
     * 和 notifyBufferAvailable 可能在不同线程中并发调用。
     *
     * @param buffer 可用的缓冲区
     * @return true 如果缓冲区被接受使用
     */
    @Override
    public boolean notifyBufferAvailable(Buffer buffer) {
        // Assuming two remote channels with respective buffer managers as listeners inside
        // LocalBufferPool.
        // While canceler thread calling ch1#releaseAllResources, it might trigger
        // bm2#notifyBufferAvaialble.
        // Concurrently if task thread is recycling exclusive buffer, it might trigger
        // bm1#notifyBufferAvailable.
        // Then these two threads will both occupy the respective bufferQueue lock and wait for
        // other side's
        // bufferQueue lock to cause deadlock. So we check the isReleased state out of synchronized
        // to resolve it.
        if (inputChannel.isReleased()) {
            return false;
        }

        int numBuffers = 0;
        boolean isBufferUsed = false;
        try {
            synchronized (bufferQueue) {
                checkState(
                        isWaitingForFloatingBuffers,
                        "This channel should be waiting for floating buffers.");
                isWaitingForFloatingBuffers = false;

                // Important: make sure that we never add a buffer after releaseAllResources()
                // released all buffers. Following scenarios exist:
                // 1) releaseAllBuffers() already released buffers inside bufferQueue
                // -> while isReleased is set correctly in InputChannel
                // 2) releaseAllBuffers() did not yet release buffers from bufferQueue
                // -> we may or may not have set isReleased yet but will always wait for the
                // lock on bufferQueue to release buffers
                if (inputChannel.isReleased()
                        || bufferQueue.getAvailableBufferSize() >= numRequiredBuffers) {
                    return false;
                }

                bufferQueue.addFloatingBuffer(buffer);
                isBufferUsed = true;
                numBuffers += 1 + tryRequestBuffers();
                bufferQueue.notifyAll();
            }

            inputChannel.notifyBufferAvailable(numBuffers);
        } catch (Throwable t) {
            inputChannel.setError(t);
        }

        return isBufferUsed;
    }

    @Override
    public void notifyBufferDestroyed() {
        // Nothing to do actually.
    }

    // ------------------------------------------------------------------------
    // Getter properties
    // ------------------------------------------------------------------------

    @VisibleForTesting
    int unsynchronizedGetNumberOfRequiredBuffers() {
        return numRequiredBuffers;
    }

    int getNumberOfRequiredBuffers() {
        synchronized (bufferQueue) {
            return numRequiredBuffers;
        }
    }

    @VisibleForTesting
    boolean unsynchronizedIsWaitingForFloatingBuffers() {
        return isWaitingForFloatingBuffers;
    }

    @VisibleForTesting
    int getNumberOfAvailableBuffers() {
        synchronized (bufferQueue) {
            return bufferQueue.getAvailableBufferSize();
        }
    }

    int unsynchronizedGetAvailableExclusiveBuffers() {
        return bufferQueue.exclusiveBuffers.size();
    }

    int unsynchronizedGetFloatingBuffersAvailable() {
        return bufferQueue.floatingBuffers.size();
    }

    /**
     * 管理该 Channel 的独占缓冲区和浮动缓冲区，并处理内部缓冲区相关逻辑。
     *
     * <p>核心设计：
     * <ul>
     *   <li>独占缓冲区：Channel 专属，生命周期与 Channel 一致</li>
     *   <li>浮动缓冲区：从 BufferPool 动态借用，可能被释放回 BufferPool</li>
     *   <li>消费优先级：优先消费浮动缓冲区，保留独占缓冲区作为保底</li>
     * </ul>
     */
    static final class AvailableBufferQueue {

        /** 从本地 BufferPool 请求的浮动缓冲区队列 */
        final ArrayDeque<Buffer> floatingBuffers;

        /** 从全局 NetworkBufferPool 分配的独占缓冲区队列 */
        final ArrayDeque<Buffer> exclusiveBuffers;

        AvailableBufferQueue() {
            this.exclusiveBuffers = new ArrayDeque<>();
            this.floatingBuffers = new ArrayDeque<>();
        }

        /**
         * 添加独占缓冲区到队列。
         *
         * <p>如果添加后可用缓冲区数量超过需求，会释放一个浮动缓冲区。
         * 这确保了独占缓冲区的优先级：当独占缓冲区回收时，多余的浮动缓冲区
         * 可以归还给 BufferPool 供其他 Channel 使用。
         *
         * @param buffer 要添加的独占缓冲区
         * @param numRequiredBuffers 当前需要的缓冲区数量
         * @return 被释放的浮动缓冲区，调用者负责回收；如果不需要释放则返回 null
         */
        @Nullable
        Buffer addExclusiveBuffer(Buffer buffer, int numRequiredBuffers) {
            exclusiveBuffers.add(buffer);
            if (getAvailableBufferSize() > numRequiredBuffers) {
                return floatingBuffers.poll();
            }
            return null;
        }

        void addFloatingBuffer(Buffer buffer) {
            floatingBuffers.add(buffer);
        }

        /**
         * 获取一个可用缓冲区。
         *
         * <p>优先返回浮动缓冲区，以充分利用动态分配的资源，
         * 保留独占缓冲区作为保底。
         *
         * @return 可用缓冲区，如果 Channel 已释放则返回 null
         */
        @Nullable
        Buffer takeBuffer() {
            if (floatingBuffers.size() > 0) {
                return floatingBuffers.poll();
            } else {
                return exclusiveBuffers.poll();
            }
        }

        /**
         * 释放所有缓冲区。
         *
         * <p>释放策略不同：
         * <ul>
         *   <li>浮动缓冲区：直接回收到 BufferPool</li>
         *   <li>独占缓冲区：只提取 MemorySegment 加入列表，由调用者批量回收到全局池</li>
         * </ul>
         *
         * @param exclusiveSegments 用于收集独占缓冲区的内存段列表
         */
        void releaseAll(List<MemorySegment> exclusiveSegments) {
            Buffer buffer;
            while ((buffer = floatingBuffers.poll()) != null) {
                buffer.recycleBuffer();
            }
            while ((buffer = exclusiveBuffers.poll()) != null) {
                exclusiveSegments.add(buffer.getMemorySegment());
            }
        }

        Queue<Buffer> clearFloatingBuffers() {
            Queue<Buffer> buffers = new ArrayDeque<>(floatingBuffers);
            floatingBuffers.clear();
            return buffers;
        }

        int getAvailableBufferSize() {
            return floatingBuffers.size() + exclusiveBuffers.size();
        }
    }
}
