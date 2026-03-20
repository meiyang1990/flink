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

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

/**
 * This class is responsible for managing all {@link BlockedNode}s and performing them on resources.
 *
 * <p>【学习型注释】
 * BlocklistHandler 是屏蔽列表管理器的核心接口，负责故障节点的记录、查询和通知。
 *
 * <p>核心职责：
 * 1. 维护屏蔽节点列表：添加新屏蔽节点，自动合并重复记录
 * 2. 查询屏蔽状态：判断 TaskManager 是否在屏蔽节点上运行
 * 3. 监听器管理：注册/注销屏蔽列表变更监听器
 * 4. 资源协调：通过 BlocklistContext 执行资源屏蔽/解除操作
 *
 * <p>实现类：
 * - DefaultBlocklistHandler：默认实现，集成了 BlocklistTracker 和超时清理机制
 * - NoOpBlocklistHandler：空实现，用于禁用屏蔽功能
 *
 * <p>使用场景：
 * ResourceManager 持有 BlocklistHandler 实例，当 TaskManager 频繁失败时，
 * 会将其所在节点加入屏蔽列表，避免新的 Task 被调度到该节点。
 */
public interface BlocklistHandler {

    /**
     * Add new blocked node records. If a node (identified by node id) already exists, the newly
     * added one will be merged with the existing one.
     *
     * @param newNodes the new blocked node records
     */
    void addNewBlockedNodes(Collection<BlockedNode> newNodes);

    /**
     * Returns whether the given task manager is blocked (located on blocked nodes).
     *
     * @param taskManagerId ID of the task manager to query
     * @return true if the given task manager is blocked, otherwise false
     */
    boolean isBlockedTaskManager(ResourceID taskManagerId);

    /**
     * Get all blocked node ids.
     *
     * @return a set containing all blocked node ids
     */
    Set<String> getAllBlockedNodeIds();

    /**
     * Register a new blocklist listener.
     *
     * @param blocklistListener the newly registered listener
     */
    void registerBlocklistListener(BlocklistListener blocklistListener);

    /**
     * Deregister a blocklist listener.
     *
     * @param blocklistListener the listener to deregister
     */
    void deregisterBlocklistListener(BlocklistListener blocklistListener);

    /** Factory to instantiate {@link BlocklistHandler}. */
    interface Factory {

        /**
         * Instantiates a {@link BlocklistHandler}.
         *
         * @param blocklistContext the blocklist context
         * @param taskManagerNodeIdRetriever to map a task manager to the node it's located on
         * @param mainThreadExecutor to schedule the timeout check
         * @param log the logger
         * @return an instantiated blocklist handler.
         */
        BlocklistHandler create(
                BlocklistContext blocklistContext,
                Function<ResourceID, String> taskManagerNodeIdRetriever,
                ComponentMainThreadExecutor mainThreadExecutor,
                Logger log);
    }
}
