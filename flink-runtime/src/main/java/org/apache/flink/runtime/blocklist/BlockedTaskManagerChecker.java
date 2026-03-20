/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

/**
 * This checker helps to query whether a given task manager is blocked.
 *
 * <p>【学习型注释】
 * BlockedTaskManagerChecker 用于检查某个 TaskManager 是否在屏蔽节点上运行。
 *
 * <p>核心方法：
 * isBlockedTaskManager(ResourceID): 检查 TaskManager 是否被屏蔽
 *
 * <p>使用场景：
 * SlotManager 在分配 Slot 时会调用此接口，确保不会将 Task 分配到屏蔽节点上的 TaskManager。
 *
 * <p>实现类：
 * BlocklistHandler：维护节点ID到屏蔽状态的映射
 */
public interface BlockedTaskManagerChecker {

    /**
     * Returns whether the given task manager is located on a blocked node.
     *
     * @param resourceID ID of the task manager to query
     * @return True if the given task manager is located on a blocked node, otherwise false.
     */
    boolean isBlockedTaskManager(ResourceID resourceID);
}
