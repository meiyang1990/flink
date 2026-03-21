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

/**
 * The resource (e.g., local files, remote storage files, etc.) for the Tiered Storage.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageResource 是分层存储中资源的抽象接口，统一管理各类资源的生命周期。
 * 典型的资源包括：
 *
 * <ul>
 *   <li><b>本地文件资源</b>: 磁盘层产生的临时数据文件</li>
 *   <li><b>远程存储资源</b>: 外部存储系统（如 HDFS、S3）中的文件引用</li>
 *   <li><b>网络连接资源</b>: 与远程服务的连接句柄</li>
 * </ul>
 *
 * <h2>使用模式</h2>
 *
 * <pre>
 *   资源创建阶段:
 *   ─────────────
 *   TierProducerAgent
 *        │
 *        │ 创建资源
 *        ▼
 *   TieredStorageResource resource = new XxxResource(...)
 *        │
 *        │ 注册到 Registry
 *        ▼
 *   TieredStorageResourceRegistry.registerResource(dataId, resource)
 *
 *   资源释放阶段:
 *   ─────────────
 *   ResultPartition.release()
 *        │
 *        │ 触发清理
 *        ▼
 *   TieredStorageResourceRegistry.clearResourceFor(dataId)
 *        │
 *        │ 遍历所有关联资源
 *        ▼
 *   TieredStorageResource.release()  ◄─ 删除文件/关闭连接
 * </pre>
 */
public interface TieredStorageResource {

    /**
     * Release all the resources, e.g. delete the files, recycle the occupied memory, etc.
     *
     * <p>释放此资源占用的所有底层资源，例如：
     * <ul>
     *   <li>删除临时文件</li>
     *   <li>关闭文件句柄</li>
     *   <li>断开网络连接</li>
     *   <li>回收内存</li>
     * </ul>
     *
     * <p>此方法应该是幂等的，多次调用不应产生副作用。
     */
    void release();
}
