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

/**
 * This class is responsible for blocking and unblocking resources.
 *
 * <p>【学习型注释】
 * BlocklistContext 是屏蔽资源的上下文接口，负责在节点被屏蔽/解除屏蔽时执行资源操作。
 *
 * <p>核心方法：
 * - blockResources: 当节点被屏蔽时，释放该节点上的资源（如已分配的Slot）
 * - unblockResources: 当节点解除屏蔽时，恢复该节点上的资源可用性
 *
 * <p>实现类：
 * - ResourceManager：作为 BlocklistContext，管理 TaskManager 注册信息和 Slot 状态
 *
 * <p>设计意图：
 * 解耦屏蔽列表管理和资源管理。BlocklistHandler 负责维护屏蔽状态，
 * BlocklistContext 负责执行具体的资源操作。
 */
public interface BlocklistContext {

    /**
     * Block resources on the nodes.
     *
     * @param blockedNodes the nodes to block resources
     */
    void blockResources(Collection<BlockedNode> blockedNodes);

    /**
     * Unblock resources on the nodes.
     *
     * @param unblockedNodes the nodes to unblock resources
     */
    void unblockResources(Collection<BlockedNode> unblockedNodes);
}
