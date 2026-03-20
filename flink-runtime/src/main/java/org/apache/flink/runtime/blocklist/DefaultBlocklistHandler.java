/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// 这个文件已经全部加上中文注释

package org.apache.flink.runtime.blocklist;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link BlocklistHandler}.
 *
 * <p>【学习型注释】
 * DefaultBlocklistHandler 是 BlocklistHandler 的默认实现，提供完整的屏蔽节点管理功能。
 *
 * <p>核心组件：
 * - BlocklistTracker：维护屏蔽节点的状态存储
 * - BlocklistContext：执行资源屏蔽/解除操作
 * - taskManagerNodeIdRetriever：将 TaskManager ID 映射到节点 ID
 * - blocklistListeners：监听器集合，通知屏蔽列表变更
 *
 * <p>关键机制：
 * 1. 超时清理：定期检查并移除过期的屏蔽节点
 * 2. 节点合并：新屏蔽节点与已存在节点自动合并（延长屏蔽时长）
 * 3. 通知机制：屏蔽列表变更时通知所有监听器
 *
 * <p>线程安全：
 * 所有方法必须在主线程执行，通过 assertRunningInMainThread 检查。
 */
public class DefaultBlocklistHandler implements BlocklistHandler, AutoCloseable {

    private final Logger log;

    private final Function<ResourceID, String> taskManagerNodeIdRetriever;

    private final BlocklistTracker blocklistTracker;

    private final BlocklistContext blocklistContext;

    private final Set<BlocklistListener> blocklistListeners = new HashSet<>();

    private final Duration timeoutCheckInterval;

    private volatile ScheduledFuture<?> timeoutCheckFuture;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    DefaultBlocklistHandler(
            BlocklistTracker blocklistTracker,
            BlocklistContext blocklistContext,
            Function<ResourceID, String> taskManagerNodeIdRetriever,
            Duration timeoutCheckInterval,
            ComponentMainThreadExecutor mainThreadExecutor,
            Logger log) {
        this.blocklistTracker = checkNotNull(blocklistTracker);
        this.blocklistContext = checkNotNull(blocklistContext);
        this.taskManagerNodeIdRetriever = checkNotNull(taskManagerNodeIdRetriever);
        this.timeoutCheckInterval = checkNotNull(timeoutCheckInterval);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
        this.log = checkNotNull(log);

        // 启动后立即开始定时清理任务
        scheduleTimeoutCheck();
    }

    /**
     * 调度定时清理任务。
     * 每隔 timeoutCheckInterval 检查一次是否有屏蔽节点超时，如果有则移除并解除资源限制。
     */
    private void scheduleTimeoutCheck() {
        this.timeoutCheckFuture =
                mainThreadExecutor.schedule(
                        () -> {
                            removeTimeoutNodes();
                            scheduleTimeoutCheck(); // 递归调度下一次清理
                        },
                        timeoutCheckInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    /**
     * 移除所有已超时的屏蔽节点。
     * 超时节点会被从 BlocklistTracker 中删除，并通知 BlocklistContext 解除资源限制。
     */
    private void removeTimeoutNodes() {
        assertRunningInMainThread();
        Collection<BlockedNode> removedNodes =
                blocklistTracker.removeTimeoutNodes(System.currentTimeMillis());
        if (!removedNodes.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Remove {} timeout blocked nodes, details {}. "
                                + "Total {} blocked nodes currently, details: {}.",
                        removedNodes.size(),
                        removedNodes,
                        blocklistTracker.getAllBlockedNodes().size(),
                        blocklistTracker.getAllBlockedNodes());
            } else {
                log.info(
                        "Remove {} timeout blocked nodes. Total {} blocked nodes currently.",
                        removedNodes.size(),
                        blocklistTracker.getAllBlockedNodes().size());
            }
            // 通知 BlocklistContext 解除这些节点的资源限制
            blocklistContext.unblockResources(removedNodes);
        }
    }

    private void assertRunningInMainThread() {
        mainThreadExecutor.assertRunningInMainThread();
    }

    /**
     * 添加新的屏蔽节点记录。
     *
     * <p>处理逻辑：
     * 1. 将新节点添加到 BlocklistTracker（自动合并已存在的节点）
     * 2. 如果有新增节点，通知所有监听器并执行资源屏蔽
     * 3. 如果只是合并节点（延长屏蔽时间），仅通知监听器
     */
    @Override
    public void addNewBlockedNodes(Collection<BlockedNode> newNodes) {
        assertRunningInMainThread();

        if (newNodes.isEmpty()) {
            return;
        }

        // 将新节点添加到 tracker，返回新增和合并的节点
        BlockedNodeAdditionResult result = blocklistTracker.addNewBlockedNodes(newNodes);
        Collection<BlockedNode> newlyAddedNodes = result.getNewlyAddedNodes();
        Collection<BlockedNode> allNodes =
                Stream.concat(newlyAddedNodes.stream(), result.getMergedNodes().stream())
                        .collect(Collectors.toList());

        if (!newlyAddedNodes.isEmpty()) {
            // 有新增节点：记录日志、通知监听器、屏蔽资源
            if (log.isDebugEnabled()) {
                log.debug(
                        "Newly added {} blocked nodes, details: {}."
                                + " Total {} blocked nodes currently, details: {}.",
                        newlyAddedNodes.size(),
                        newlyAddedNodes,
                        blocklistTracker.getAllBlockedNodes().size(),
                        blocklistTracker.getAllBlockedNodes());
            } else {
                log.info(
                        "Newly added {} blocked nodes. Total {} blocked nodes currently.",
                        newlyAddedNodes.size(),
                        blocklistTracker.getAllBlockedNodes().size());
            }

            // 通知所有监听器（包括新增和合并的节点）
            blocklistListeners.forEach(listener -> listener.notifyNewBlockedNodes(allNodes));
            // 对新增节点执行资源屏蔽（释放 Slot 等）
            blocklistContext.blockResources(newlyAddedNodes);
        } else if (!allNodes.isEmpty()) {
            // 仅合并节点：只通知监听器，不重复执行资源屏蔽
            blocklistListeners.forEach(listener -> listener.notifyNewBlockedNodes(allNodes));
        }
    }

    @Override
    public boolean isBlockedTaskManager(ResourceID taskManagerId) {
        assertRunningInMainThread();
        String nodeId = checkNotNull(taskManagerNodeIdRetriever.apply(taskManagerId));
        return blocklistTracker.isBlockedNode(nodeId);
    }

    @Override
    public Set<String> getAllBlockedNodeIds() {
        assertRunningInMainThread();

        return blocklistTracker.getAllBlockedNodeIds();
    }

    /**
     * 注册屏蔽列表监听器。
     *
     * <p>注册后会立即通知该监听器当前的屏蔽节点列表，
     * 确保新注册的监听器不会错过已有的屏蔽信息。
     */
    @Override
    public void registerBlocklistListener(BlocklistListener blocklistListener) {
        assertRunningInMainThread();

        checkNotNull(blocklistListener);
        if (!blocklistListeners.contains(blocklistListener)) {
            blocklistListeners.add(blocklistListener);
            // 立即通知新监听器当前的屏蔽节点列表
            Collection<BlockedNode> allBlockedNodes = blocklistTracker.getAllBlockedNodes();
            if (!allBlockedNodes.isEmpty()) {
                blocklistListener.notifyNewBlockedNodes(allBlockedNodes);
            }
        }
    }

    @Override
    public void deregisterBlocklistListener(BlocklistListener blocklistListener) {
        assertRunningInMainThread();

        checkNotNull(blocklistListener);
        blocklistListeners.remove(blocklistListener);
    }

    @Override
    public void close() throws Exception {
        if (timeoutCheckFuture != null) {
            timeoutCheckFuture.cancel(false);
        }
    }

    /** The factory to instantiate {@link DefaultBlocklistHandler}. */
    public static class Factory implements BlocklistHandler.Factory {

        private final Duration timeoutCheckInterval;

        public Factory(Duration timeoutCheckInterval) {
            this.timeoutCheckInterval = checkNotNull(timeoutCheckInterval);
        }

        @Override
        public BlocklistHandler create(
                BlocklistContext blocklistContext,
                Function<ResourceID, String> taskManagerNodeIdRetriever,
                ComponentMainThreadExecutor mainThreadExecutor,
                Logger log) {
            return new DefaultBlocklistHandler(
                    new DefaultBlocklistTracker(),
                    blocklistContext,
                    taskManagerNodeIdRetriever,
                    timeoutCheckInterval,
                    mainThreadExecutor,
                    log);
        }
    }
}
