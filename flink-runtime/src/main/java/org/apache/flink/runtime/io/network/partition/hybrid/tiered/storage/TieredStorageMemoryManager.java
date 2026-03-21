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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import java.util.List;

/**
 * The {@link TieredStorageMemoryManager} is to request or recycle buffers from {@link
 * LocalBufferPool} for different memory owners, for example, the tiers, the buffer accumulator,
 * etc. Note that the logic for requesting and recycling buffers is consistent for these owners.
 *
 * <p>The buffers managed by {@link TieredStorageMemoryManager} is categorized into two types:
 * <b>non-reclaimable</b> buffers which cannot be immediately released and <b>reclaimable
 * buffers</b> which can be reclaimed quickly and safely. Non-reclaimable buffers necessitates
 * waiting for other operations to complete before releasing it, such as downstream consumption. On
 * the other hand, reclaimable buffers can be freed up at any time, enabling rapid memory recycling
 * for tasks such as flushing memory to disk or remote storage.
 *
 * <p>The {@link TieredStorageMemoryManager} does not provide strict memory limitations on any user
 * can request. Instead, it only simply provides memory usage hints to memory users. It is very
 * <b>important</b> to note that <b>only</b> users with non-reclaimable should check the memory
 * hints by calling {@code getMaxNonReclaimableBuffers} before requesting buffers.
 *
 * <p>The {@link TieredStorageMemoryManager} needs to ensure that it would not hinder reclaimable
 * users from acquiring buffers due to non-reclaimable users not releasing the buffers they have
 * requested. So it is very <b>important</b> to note that <b>only</b> users with non-reclaimable
 * should call {@code ensureCapacity} before requesting buffers to reserve enough buffers.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>Tiered Storage 内存管理器，负责为不同的内存所有者（如各存储层、缓冲累积器等）从 {@link LocalBufferPool}
 * 请求和回收缓冲区。
 *
 * <h3>缓冲区分类</h3>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                    TieredStorageMemoryManager                   │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                                                                 │
 * │  ┌─────────────────────┐    ┌─────────────────────────────────┐│
 * │  │  Non-Reclaimable    │    │      Reclaimable Buffers        ││
 * │  │  不可回收缓冲区      │    │      可回收缓冲区                ││
 * │  ├─────────────────────┤    ├─────────────────────────────────┤│
 * │  │ - 正在被下游消费     │    │ - 可随时释放                     ││
 * │  │ - 需等待操作完成     │    │ - 用于刷盘或远程存储             ││
 * │  │ - 典型：Memory Tier │    │ - 典型：Disk/Remote Tier         ││
 * │  └─────────────────────┘    └─────────────────────────────────┘│
 * │                                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>使用约束</h3>
 * <ul>
 *   <li>Non-Reclaimable 用户：请求前必须调用 {@code getMaxNonReclaimableBuffers} 检查配额
 *   <li>Non-Reclaimable 用户：请求前必须调用 {@code ensureCapacity} 预留缓冲区
 *   <li>Reclaimable 用户：可直接请求缓冲区，无需额外检查
 * </ul>
 */
public interface TieredStorageMemoryManager {

    /**
     * Setup the {@link TieredStorageMemoryManager}. When setting up the manager, the {@link
     * TieredStorageMemorySpec}s for different tiered storages should be ready to indicate each
     * tiered storage's memory requirement specs.
     *
     * <p>初始化内存管理器，绑定 BufferPool 并注册各存储层的内存规格。
     * 只有完成初始化后，各存储层才能从该管理器请求缓冲区。
     *
     * @param bufferPool the local buffer pool
     * @param storageMemorySpecs the memory specs for different tiered storages
     */
    void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs);

    /**
     * Set the {@link TaskIOMetricGroup} for this memory manager.
     *
     * <p>设置指标组，用于收集硬背压时间等监控指标。
     *
     * @param metricGroup the metric group to set
     */
    void setMetricGroup(TaskIOMetricGroup metricGroup);

    /**
     * Register a listener to listen the buffer reclaim request from the {@link
     * TieredStorageMemoryManager}.
     *
     * <p>When the left buffers in the {@link BufferPool} are not enough, {@link
     * TieredStorageMemoryManager} will try to reclaim the buffers from the memory owners.
     *
     * <p>注册缓冲区回收监听器。当 BufferPool 中剩余缓冲区不足时，
     * 内存管理器会通知各所有者释放可回收的缓冲区。
     *
     * @param onBufferReclaimRequest a {@link Runnable} to process the buffer reclaim request
     */
    void listenBufferReclaimRequest(Runnable onBufferReclaimRequest);

    /**
     * Expose and get the internal {@link BufferPool}. Please note that this method is a temporary
     * workaround for the remote tier plugin and may be removed at any time in the future. We
     * strongly advise that users do not rely on this method.
     *
     * <p>获取内部 BufferPool 的临时方法，供远程存储层插件使用。
     * 警告：此方法为临时解决方案，未来可能被移除。
     */
    BufferPool getBufferPool();

    /**
     * Request a {@link BufferBuilder} instance for a specific owner. The {@link
     * TieredStorageMemoryManagerImpl} will not check whether a buffer can be requested. The manager
     * only records the number of requested buffers. If the buffers is not enough to meet the
     * request, the manager will request each tiered storage to reclaim their requested buffers as
     * much as possible.
     *
     * <p>This is not thread safe and is expected to be called only from the task thread.
     *
     * <p>以阻塞方式为指定所有者请求一个 BufferBuilder。
     * 若缓冲区不足，会触发回收机制尝试释放可回收缓冲区。
     * 注意：此方法非线程安全，仅应从 Task 线程调用。
     *
     * @param owner the owner to request buffer
     * @return the requested buffer
     */
    BufferBuilder requestBufferBlocking(Object owner);

    /**
     * Return the number of the non-reclaimable buffers for the owner.
     *
     * <p>Note that the available buffers are calculated dynamically based on some conditions, for
     * example, the state of the {@link BufferPool}, the {@link TieredStorageMemorySpec} of the
     * owner, etc. So the caller should always check before requesting non-reclaimable buffers.
     *
     * <p>When invoking this method, the caller should be aware that the return value may
     * occasionally be negative. This is due to the possibility of the buffer pool size shrinking to
     * a point where it is smaller than the buffers owned by other users. In such cases, the maximum
     * non-reclaimable buffer value returned may be negative.
     *
     * <p>获取指定所有者可使用的最大不可回收缓冲区数量。
     * 该值动态计算，考虑 BufferPool 状态和其他所有者的配额。
     * 注意：当 BufferPool 缩容时返回值可能为负数。
     */
    int getMaxNonReclaimableBuffers(Object owner);

    /**
     * Try best to reserve enough buffers that are guaranteed reclaimable along with the additional
     * ones.
     *
     * <p>Note that the available buffers are calculated dynamically based on some conditions, for
     * example, the state of the {@link BufferPool}, the {@link TieredStorageMemorySpec} of the
     * owner, etc. So the caller should always ensure capacity before requesting non-reclaimable
     * buffers.
     *
     * <p>预留足够的可回收缓冲区以及额外请求的缓冲区。
     * 不可回收缓冲区的所有者必须在请求前调用此方法确保容量。
     *
     * @param numAdditionalBuffers the number of buffers that need to also be reserved in addition
     *     to guaranteed reclaimable buffers.
     * @return True if the capacity meets the requirements, false otherwise.
     */
    boolean ensureCapacity(int numAdditionalBuffers);

    /**
     * Return the number of requested buffers belonging to a specific owner.
     *
     * <p>获取指定所有者已请求的缓冲区数量。
     *
     * @param owner the owner of requesting buffers
     * @return the number of requested buffers belonging to the owner.
     */
    int numOwnerRequestedBuffer(Object owner);

    /**
     * Notify the memory manager that transferring one buffer's ownership from the old owner to the
     * new owner.
     *
     * <p>转移缓冲区的所有权，同时更新 recycler 回调。
     * 用于缓冲区在不同存储层之间流转的场景。
     *
     * @param oldOwner the old owner of one buffer
     * @param newOwner the new owner of one buffer
     * @param buffer the buffer to transfer the ownership
     */
    void transferBufferOwnership(Object oldOwner, Object newOwner, Buffer buffer);

    /**
     * Release all the resources(if exists) and check the state of the {@link
     * TieredStorageMemoryManager}.
     *
     * <p>释放所有资源，将内部缓冲队列中的缓冲区归还给 BufferPool。
     */
    void release();
}
