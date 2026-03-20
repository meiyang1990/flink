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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link BlocklistTracker}.
 *
 * <p>【学习型注释】
 * DefaultBlocklistTracker 是 BlocklistTracker 的默认实现，使用 HashMap 存储屏蔽节点。
 *
 * <p>数据结构：
 * blockedNodes: Map<节点ID, 屏蔽记录>，以节点ID为键，支持O(1)查询
 *
 * <p>核心功能：
 * 1. 添加节点：支持新增和合并（延长屏蔽时长）
 * 2. 查询状态：判断节点是否在屏蔽列表中
 * 3. 清理过期：定期移除超时的屏蔽记录
 *
 * <p>合并策略：
 * 当同一节点被多次屏蔽时，选择屏蔽结束时间更晚的记录，
 * 确保屏蔽时长能够覆盖所有报告的故障。
 */
public class DefaultBlocklistTracker implements BlocklistTracker {
    private final Map<String, BlockedNode> blockedNodes = new HashMap<>();

    /**
     * 尝试添加或合并屏蔽节点记录。
     *
     * <p>处理逻辑：
     * - 节点不存在：直接添加，返回 ADDED
     * - 节点已存在且新记录结束时间更晚：更新为新记录，返回 MERGED
     * - 节点已存在但新记录更早：保持原记录，返回 NONE
     *
     * @param newNode 新的屏蔽节点记录
     * @return 添加状态（ADDED/MERGED/NONE）
     */
    private AddStatus tryAddOrMerge(BlockedNode newNode) {
        checkNotNull(newNode);
        final String nodeId = newNode.getNodeId();
        final BlockedNode existingNode = blockedNodes.get(nodeId);

        if (existingNode == null) {
            // 节点不存在，直接添加
            blockedNodes.put(nodeId, newNode);
            return AddStatus.ADDED;
        } else {
            // 节点已存在，选择屏蔽时间更长的记录
            BlockedNode merged =
                    newNode.getEndTimestamp() >= existingNode.getEndTimestamp()
                            ? newNode
                            : existingNode;
            if (!merged.equals(existingNode)) {
                // 新记录更长，更新屏蔽信息
                blockedNodes.put(nodeId, merged);
                return AddStatus.MERGED;
            }
            // 原记录更长或相同，无需更新
            return AddStatus.NONE;
        }
    }

    /**
     * 批量添加新的屏蔽节点记录。
     *
     * <p>遍历所有新节点，逐一调用 tryAddOrMerge 处理，
     * 统计新增节点和合并节点，返回详细结果。
     */
    @Override
    public BlockedNodeAdditionResult addNewBlockedNodes(Collection<BlockedNode> newNodes) {
        checkNotNull(newNodes);

        final Map<String, BlockedNode> newlyAddedNodes = new HashMap<>();
        final Map<String, BlockedNode> mergedNodes = new HashMap<>();
        for (BlockedNode node : newNodes) {
            String nodeId = node.getNodeId();
            AddStatus status = tryAddOrMerge(node);
            switch (status) {
                case ADDED:
                    newlyAddedNodes.put(nodeId, blockedNodes.get(nodeId));
                    break;
                case MERGED:
                    mergedNodes.put(nodeId, blockedNodes.get(nodeId));
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalStateException(
                            "Add or merge status " + status + " is not supported.");
            }
        }
        return new BlockedNodeAdditionResult(newlyAddedNodes.values(), mergedNodes.values());
    }

    @Override
    public boolean isBlockedNode(String nodeId) {
        checkNotNull(nodeId);
        return blockedNodes.containsKey(nodeId);
    }

    @Override
    public Set<String> getAllBlockedNodeIds() {
        return Collections.unmodifiableSet(blockedNodes.keySet());
    }

    @Override
    public Collection<BlockedNode> getAllBlockedNodes() {
        return Collections.unmodifiableCollection(blockedNodes.values());
    }

    /**
     * 移除所有已超时的屏蔽节点。
     *
     * <p>遍历所有屏蔽节点，检查当前时间是否已超过屏蔽结束时间。
     * 超时的节点会被移除并返回，用于后续通知 BlocklistContext 解除资源限制。
     *
     * @param currentTimestamp 当前时间戳（毫秒）
     * @return 被移除的屏蔽节点集合
     */
    @Override
    public Collection<BlockedNode> removeTimeoutNodes(long currentTimestamp) {
        Collection<BlockedNode> removedNodes = new ArrayList<>();
        final Iterator<BlockedNode> blockedNodeIterator = blockedNodes.values().iterator();
        while (blockedNodeIterator.hasNext()) {
            BlockedNode blockedNode = blockedNodeIterator.next();
            // 当前时间超过屏蔽结束时间，移除该节点
            if (currentTimestamp >= blockedNode.getEndTimestamp()) {
                removedNodes.add(blockedNode);
                blockedNodeIterator.remove();
            }
        }
        return removedNodes;
    }

    private enum AddStatus {
        ADDED,
        MERGED,
        NONE
    }
}
