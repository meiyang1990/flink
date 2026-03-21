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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageDataIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A registry that maintains local or remote resources that correspond to a certain set of data in
 * the Tiered Storage.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageResourceRegistry 是分层存储的资源注册表，用于管理与特定数据集关联的本地或远程资源。
 * 当数据不再需要时，可以通过此注册表释放所有相关资源。
 *
 * <h2>主要职责</h2>
 *
 * <ul>
 *   <li><b>资源注册</b>: 将资源（如文件句柄、网络连接、内存缓冲区）与数据标识符关联</li>
 *   <li><b>资源清理</b>: 按数据标识符批量释放所有关联资源</li>
 *   <li><b>生命周期管理</b>: 确保数据删除时其占用的资源被正确回收</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <pre>
 *   数据写入流程:
 *   ─────────────
 *   TieredStorageProducerClient
 *           │
 *           │ 创建存储资源（文件/缓冲区）
 *           ▼
 *   ┌───────────────────────────┐
 *   │ TieredStorageResourceRegistry │
 *   │   registerResource(owner, res) │
 *   └───────────────────────────┘
 *
 *   数据清理流程:
 *   ─────────────
 *   ResultPartition.release()
 *           │
 *           │ 数据不再需要
 *           ▼
 *   ┌───────────────────────────┐
 *   │ TieredStorageResourceRegistry │
 *   │   clearResourceFor(owner)     │ ─────► 释放所有关联资源
 *   └───────────────────────────┘
 * </pre>
 *
 * <h2>典型资源类型</h2>
 *
 * <ul>
 *   <li>Disk Tier: 临时文件、文件通道</li>
 *   <li>Remote Tier: 网络连接、远程文件引用</li>
 *   <li>Memory Tier: 内存缓冲区（通常由 MemoryManager 管理，此处不注册）</li>
 * </ul>
 */
public class TieredStorageResourceRegistry {

    // 资源映射表: 数据标识符 -> 该数据关联的所有资源列表
    // 使用 HashMap 存储，每个 owner 可以有多个资源
    private final Map<TieredStorageDataIdentifier, List<TieredStorageResource>>
            registeredResources = new HashMap<>();

    /**
     * Register a new resource for the given owner.
     *
     * <p>为指定的数据所有者注册新资源。同一 owner 可以注册多个资源。
     *
     * @param owner identifier of the data that the resource corresponds to.
     *              资源对应的数据标识符（作为资源所有者）
     * @param tieredStorageResource the tiered storage resources to be registered.
     *              待注册的分层存储资源
     */
    public void registerResource(
            TieredStorageDataIdentifier owner, TieredStorageResource tieredStorageResource) {
        // computeIfAbsent 确保 owner 对应的列表存在
        registeredResources
                .computeIfAbsent(owner, (ignore) -> new ArrayList<>())
                .add(tieredStorageResource);
    }

    /**
     * Remove all resources for the given owner.
     *
     * <p>移除并释放指定 owner 的所有资源。在数据删除或分区释放时调用。
     *
     * @param owner identifier of the data that the resources correspond to.
     *              资源对应的数据标识符
     */
    public void clearResourceFor(TieredStorageDataIdentifier owner) {
        // 移除并获取该 owner 的所有资源
        List<TieredStorageResource> cleanersForOwner = registeredResources.remove(owner);

        // 逐个释放资源
        if (cleanersForOwner != null) {
            cleanersForOwner.forEach(TieredStorageResource::release);
        }
    }
}
