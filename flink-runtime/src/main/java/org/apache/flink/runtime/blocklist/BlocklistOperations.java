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
 * Operations to perform on the blocklist.
 *
 * <p>【学习型注释】
 * BlocklistOperations 定义了对屏蔽列表的操作接口，目前只包含添加新屏蔽节点。
 *
 * <p>设计意图：
 * 这是一个最小化的操作接口，允许外部组件（如 TaskManager）
 * 向 ResourceManager 报告故障节点，而无需暴露完整的 BlocklistHandler 功能。
 *
 * <p>继承关系：
 * BlocklistHandler 继承此接口，提供更完整的屏蔽列表管理能力。
 */
public interface BlocklistOperations {

    /**
     * Add new blocked node records. If a node (identified by node id) already exists, the newly
     * added one will be merged with the existing one.
     *
     * @param newNodes the new blocked node records
     */
    void addNewBlockedNodes(Collection<BlockedNode> newNodes);
}
