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
import org.apache.flink.runtime.shuffle.DefaultShuffleMetrics;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a task
 * manager.
 *
 * <h2>核心设计概述</h2>
 * <p>ResultPartitionManager 是 TaskManager 级别的分区管理器，负责管理当前 TaskManager 上
 * 所有 ResultPartition 的生命周期，是 Shuffle 数据生产者和消费者之间的桥梁。
 *
 * <h3>1. 核心职责</h3>
 * <ul>
 *   <li><b>分区注册</b>：Task 启动时注册其产出的 ResultPartition</li>
 *   <li><b>分区查找</b>：下游 Task 请求数据时查找对应的 ResultPartition</li>
 *   <li><b>视图创建</b>：为消费者创建 ResultSubpartitionView 以读取数据</li>
 *   <li><b>监听器管理</b>：处理分区尚未就绪时的请求监听</li>
 *   <li><b>生命周期管理</b>：分区释放、超时清理、shutdown 等</li>
 * </ul>
 *
 * <h3>2. 分区请求流程</h3>
 * <pre>
 *   下游 Task 请求数据流程：
 *
 *   +------------------+        +------------------------+
 *   | PartitionRequest |  --->  | ResultPartitionManager |
 *   +------------------+        +------------------------+
 *                                        |
 *           +----------------------------+----------------------------+
 *           |                                                         |
 *           v (分区已注册)                                            v (分区未注册)
 *   +--------------------+                               +---------------------------+
 *   | createSubpartition |                               | 注册 PartitionRequest     |
 *   | View               |                               | Listener 等待分区就绪    |
 *   +--------------------+                               +---------------------------+
 *           |                                                         |
 *           v                                                         v (分区注册时)
 *   +--------------------+                               +---------------------------+
 *   | 返回 View 开始消费  |                               | notifyPartitionCreated    |
 *   +--------------------+                               +---------------------------+
 * </pre>
 *
 * <h3>3. 超时机制</h3>
 * <ul>
 *   <li>支持配置分区请求监听器的超时时间（partitionListenerTimeout）</li>
 *   <li>定期检查超时的监听器并通知超时</li>
 *   <li>防止因上游 Task 失败导致下游无限等待</li>
 * </ul>
 *
 * <h3>4. 线程安全</h3>
 * <p>通过 synchronized(registeredPartitions) 保护内部状态，支持多线程并发访问。
 *
 * @see ResultPartition
 * @see ResultPartitionProvider
 * @see PartitionRequestListener
 */
public class ResultPartitionManager implements ResultPartitionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

    /**
     * 已注册的 ResultPartition 映射表。
     *
     * <p>Key 为 ResultPartitionID，Value 为对应的 ResultPartition 实例。
     * 同时作为同步锁保护所有内部状态。
     */
    private final Map<ResultPartitionID, ResultPartition> registeredPartitions =
            CollectionUtil.newHashMapWithExpectedSize(16);

    /**
     * 分区请求监听器管理器映射表。
     *
     * <p>当请求的分区尚未注册时，将监听器存储在此，等待分区就绪后通知。
     * Key 为 ResultPartitionID，Value 为该分区的监听器管理器。
     */
    @GuardedBy("registeredPartitions")
    private final Map<ResultPartitionID, PartitionRequestListenerManager> listenerManagers =
            new HashMap<>();

    /**
     * 监听器超时检查的定时任务。
     *
     * <p>定期检查并清理超时的分区请求监听器，防止无限等待。
     */
    @Nullable private ScheduledFuture<?> partitionListenerTimeoutChecker;

    /**
     * 分区监听器超时时间（毫秒）。
     *
     * <p>如果监听器等待超过此时间仍未收到分区就绪通知，将被视为超时。
     * 值为 0 表示禁用超时检查。
     */
    private final int partitionListenerTimeout;

    /**
     * 是否已关闭。
     *
     * <p>关闭后不再接受新的分区注册。
     */
    private boolean isShutdown;

    @VisibleForTesting
    public ResultPartitionManager() {
        this(0, null);
    }

    public ResultPartitionManager(
            int partitionListenerTimeout, ScheduledExecutor scheduledExecutor) {
        this.partitionListenerTimeout = partitionListenerTimeout;
        if (partitionListenerTimeout > 0 && scheduledExecutor != null) {
            this.partitionListenerTimeoutChecker =
                    scheduledExecutor.scheduleWithFixedDelay(
                            this::checkRequestPartitionListeners,
                            partitionListenerTimeout,
                            partitionListenerTimeout,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 注册 ResultPartition 到管理器。
     *
     * <p>注册流程：
     * <ol>
     *   <li>检查管理器是否已关闭</li>
     *   <li>检查是否重复注册</li>
     *   <li>将分区加入 registeredPartitions</li>
     *   <li>如果有等待该分区的监听器，通知它们分区已就绪</li>
     * </ol>
     *
     * @param partition 待注册的 ResultPartition
     * @throws IOException 如果管理器已关闭或分区已注册
     */
    public void registerResultPartition(ResultPartition partition) throws IOException {
        PartitionRequestListenerManager listenerManager;
        synchronized (registeredPartitions) {
            checkState(!isShutdown, "Result partition manager already shut down.");

            ResultPartition previous =
                    registeredPartitions.put(partition.getPartitionId(), partition);

            if (previous != null) {
                throw new IllegalStateException("Result partition already registered.");
            }

            listenerManager = listenerManagers.remove(partition.getPartitionId());
        }
        if (listenerManager != null) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreated(partition);
            }
        }

        LOG.debug("Registered {}.", partition);
    }

    /**
     * 为指定子分区创建读取视图。
     *
     * <p>同步方法，分区必须已注册。用于下游 Task 创建数据读取通道。
     *
     * @param partitionId 目标分区 ID
     * @param subpartitionIndexSet 目标子分区索引集合
     * @param availabilityListener 数据可用性监听器，用于通知下游有新数据
     * @return 子分区读取视图
     * @throws PartitionNotFoundException 如果分区未注册
     */
    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            BufferAvailabilityListener availabilityListener)
            throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                throw new PartitionNotFoundException(partitionId);
            }

            LOG.debug("Requesting subpartitions {} of {}.", subpartitionIndexSet, partition);

            subpartitionView =
                    partition.createSubpartitionView(subpartitionIndexSet, availabilityListener);
        }

        return subpartitionView;
    }

    /**
     * 创建子分区视图，如果分区未就绪则注册监听器等待。
     *
     * <p>异步友好的方法，用于处理分区可能尚未注册的情况：
     * <ul>
     *   <li>如果分区已注册：直接创建并返回视图</li>
     *   <li>如果分区未注册：注册监听器等待，返回空 Optional</li>
     * </ul>
     *
     * @param partitionId 目标分区 ID
     * @param subpartitionIndexSet 目标子分区索引集合
     * @param availabilityListener 数据可用性监听器
     * @param partitionRequestListener 分区就绪监听器，分区注册后会收到通知
     * @return 子分区视图（如果分区已注册）或空 Optional（如果分区未注册）
     */
    @Override
    public Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener partitionRequestListener)
            throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                listenerManagers
                        .computeIfAbsent(partitionId, key -> new PartitionRequestListenerManager())
                        .registerListener(partitionRequestListener);
                subpartitionView = null;
            } else {

                LOG.debug("Requesting subpartitions {} of {}.", subpartitionIndexSet, partition);

                subpartitionView =
                        partition.createSubpartitionView(
                                subpartitionIndexSet, availabilityListener);
            }
        }

        return subpartitionView == null ? Optional.empty() : Optional.of(subpartitionView);
    }

    @Override
    public void releasePartitionRequestListener(PartitionRequestListener listener) {
        synchronized (registeredPartitions) {
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.get(listener.getResultPartitionId());
            if (listenerManager != null) {
                listenerManager.remove(listener.getReceiverId());
                if (listenerManager.isEmpty()) {
                    listenerManagers.remove(listener.getResultPartitionId());
                }
            }
        }
    }

    /**
     * 释放指定分区并通知等待的监听器超时。
     *
     * <p>分区释放流程：
     * <ol>
     *   <li>从 registeredPartitions 移除分区</li>
     *   <li>调用分区的 release 方法释放资源</li>
     *   <li>通知等待该分区的监听器超时（分区不会再就绪）</li>
     * </ol>
     *
     * @param partitionId 待释放的分区 ID
     * @param cause 释放原因（用于日志和异常传递）
     */
    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
        PartitionRequestListenerManager listenerManager;
        synchronized (registeredPartitions) {
            ResultPartition resultPartition = registeredPartitions.remove(partitionId);
            if (resultPartition != null) {
                resultPartition.release(cause);
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            listenerManager = listenerManagers.remove(partitionId);
        }
        if (listenerManager != null && !listenerManager.isEmpty()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
    }

    /**
     * 关闭管理器，释放所有分区和资源。
     *
     * <p>关闭流程：
     * <ol>
     *   <li>释放所有已注册的分区</li>
     *   <li>清空 registeredPartitions</li>
     *   <li>通知所有等待的监听器超时</li>
     *   <li>取消超时检查定时任务</li>
     *   <li>标记 isShutdown = true</li>
     * </ol>
     */
    public void shutdown() {
        synchronized (registeredPartitions) {
            LOG.debug(
                    "Releasing {} partitions because of shutdown.",
                    registeredPartitions.values().size());

            for (ResultPartition partition : registeredPartitions.values()) {
                partition.release();
            }

            registeredPartitions.clear();

            releaseListenerManagers();

            // stop the timeout checks for the TaskManagers
            if (partitionListenerTimeoutChecker != null) {
                partitionListenerTimeoutChecker.cancel(false);
                partitionListenerTimeoutChecker = null;
            }

            isShutdown = true;

            LOG.debug("Successful shutdown.");
        }
    }

    private void releaseListenerManagers() {
        for (PartitionRequestListenerManager listenerManager : listenerManagers.values()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
        listenerManagers.clear();
    }

    /**
     * Check whether the partition request listener is timeout.
     *
     * <p>定期检查分区请求监听器是否超时。由 partitionListenerTimeoutChecker 定时调用。
     *
     * <p>检查逻辑：
     * <ol>
     *   <li>遍历所有 listenerManagers</li>
     *   <li>调用 removeExpiration 移除超时的监听器</li>
     *   <li>对超时的监听器调用 notifyPartitionCreatedTimeout</li>
     *   <li>清理空的 listenerManager</li>
     * </ol>
     */
    private void checkRequestPartitionListeners() {
        List<PartitionRequestListener> timeoutPartitionRequestListeners = new LinkedList<>();
        synchronized (registeredPartitions) {
            if (isShutdown) {
                return;
            }
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<ResultPartitionID, PartitionRequestListenerManager>> iterator =
                    listenerManagers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ResultPartitionID, PartitionRequestListenerManager> entry =
                        iterator.next();
                PartitionRequestListenerManager partitionRequestListeners = entry.getValue();
                partitionRequestListeners.removeExpiration(
                        now, partitionListenerTimeout, timeoutPartitionRequestListeners);
                if (partitionRequestListeners.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        for (PartitionRequestListener partitionRequestListener : timeoutPartitionRequestListeners) {
            partitionRequestListener.notifyPartitionCreatedTimeout();
        }
    }

    @VisibleForTesting
    public Map<ResultPartitionID, PartitionRequestListenerManager> getListenerManagers() {
        return listenerManagers;
    }

    // ------------------------------------------------------------------------
    // Notifications - 通知回调
    // ------------------------------------------------------------------------

    /**
     * 分区被完全消费后的回调通知。
     *
     * <p>当分区的所有数据都被下游消费完毕后，由 ResultPartition 调用此方法通知管理器。
     * 管理器会从注册表中移除该分区并释放其资源。
     *
     * <p>注意：只有当 partition == previous 时才释放，防止误释放新注册的同 ID 分区。
     *
     * @param partition 被消费完的分区
     */
    void onConsumedPartition(ResultPartition partition) {
        LOG.debug("Received consume notification from {}.", partition);

        synchronized (registeredPartitions) {
            final ResultPartition previous =
                    registeredPartitions.remove(partition.getPartitionId());
            // Release the partition if it was successfully removed
            if (partition == previous) {
                partition.release();
                ResultPartitionID partitionId = partition.getPartitionId();
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.remove(partition.getPartitionId());
            checkState(
                    listenerManager == null || listenerManager.isEmpty(),
                    "The partition request listeners is not empty for "
                            + partition.getPartitionId());
        }
    }

    public Collection<ResultPartitionID> getUnreleasedPartitions() {
        synchronized (registeredPartitions) {
            return registeredPartitions.keySet();
        }
    }

    public Optional<ShuffleMetrics> getMetricsOfPartition(ResultPartitionID partitionId) {
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                return Optional.empty();
            }

            return Optional.of(
                    new DefaultShuffleMetrics(
                            partition.getResultPartitionBytes().createSnapshot()));
        }
    }
}
