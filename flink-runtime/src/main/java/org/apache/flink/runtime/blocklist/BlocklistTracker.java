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

import java.util.Collection;
import java.util.Set;

/**
 * A tracker for blocklist.
 *
 * <p>【学习型注释】
 * BlocklistTracker 是屏蔽列表追踪器接口，负责维护和管理屏蔽节点的状态。
 *
 * <p>核心职责：
 * 1. 添加屏蔽节点：处理新增节点，自动合并已存在的节点记录
 * 2. 查询屏蔽状态：检查节点是否在屏蔽列表中
 * 3. 清理过期节点：移除超时的屏蔽记录
 *
 * <p>实现类：
 * - DefaultBlocklistTracker：默认实现，使用 Map 存储屏蔽节点
 *
 * <p>设计意图：
 * 将屏蔽列表的存储和查询逻辑从 BlocklistHandler 中剥离，
 * BlocklistHandler 专注于协调和通知，BlocklistTracker 专注于状态管理。
 */
public interface BlocklistTracker {

    /**
     * Add new blocked node records. If a node (identified by node id) already exists, the newly
     * added one will be merged with the existing one.
     *
     * @param newNodes the new blocked node records
     * @return the addition result
     */
    BlockedNodeAdditionResult addNewBlockedNodes(Collection<BlockedNode> newNodes);

    /**
     * Returns whether the given node is blocked.
     *
     * @param nodeId ID of the node to query
     * @return true if the given node is blocked, otherwise false
     */
    boolean isBlockedNode(String nodeId);

    /**
     * Get all blocked node ids.
     *
     * @return a set containing all blocked node ids
     */
    Set<String> getAllBlockedNodeIds();

    /**
     * Get all blocked nodes.
     *
     * @return a collection containing all blocked nodes
     */
    Collection<BlockedNode> getAllBlockedNodes();

    /**
     * Remove timeout nodes.
     *
     * @param currentTimeMillis current time
     * @return the removed nodes
     */
    Collection<BlockedNode> removeTimeoutNodes(long currentTimeMillis);
}
