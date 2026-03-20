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

import java.util.Set;

/**
 * This class helps to retrieve the blocked nodes.
 *
 * <p>【学习型注释】
 * BlockedNodeRetriever 是屏蔽节点查询接口，用于获取当前所有被屏蔽的节点ID集合。
 *
 * <p>实现类：
 * - BlocklistHandler：ResourceManager 使用，管理全局屏蔽列表
 * - NoOpBlocklistHandler：空实现，用于禁用屏蔽功能
 *
 * <p>使用场景：
 * 调度器在分配 Slot 时会检查节点是否在屏蔽列表中，避免将Task调度到故障节点。
 */
public interface BlockedNodeRetriever {

    /**
     * Get all blocked node ids.
     *
     * @return a set containing all blocked node ids
     */
    Set<String> getAllBlockedNodeIds();
}
