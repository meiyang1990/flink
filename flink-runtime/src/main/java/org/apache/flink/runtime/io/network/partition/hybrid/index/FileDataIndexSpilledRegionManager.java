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

package org.apache.flink.runtime.io.network.partition.hybrid.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;

/**
 * This class is responsible for spilling region to disk and managing these spilled regions.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>溢写区域管理器负责将从内存缓存中淘汰的索引区域持久化到磁盘，并提供从磁盘查找区域的能力。
 * 这是混合 Shuffle 索引系统的磁盘层抽象。
 *
 * <h2>主要职责</h2>
 *
 * <ul>
 *   <li><b>写入</b>：将被淘汰的索引区域追加或覆盖写入索引文件
 *   <li><b>查找</b>：根据子分区 ID 和 buffer index 定位并读取索引区域
 *   <li><b>缓存协作</b>：可选地将查找到的区域加载回内存缓存
 * </ul>
 *
 * <h2>与其他组件的关系</h2>
 *
 * <pre>
 *     FileDataIndexCache
 *            │
 *            │ (淘汰时调用 appendOrOverwriteRegion)
 *            │ (未命中时调用 findRegion)
 *            ↓
 *     FileDataIndexSpilledRegionManager
 *            │
 *            │ (读写磁盘)
 *            ↓
 *        索引文件 (index file)
 * </pre>
 *
 * @param <T> Region 类型
 */
public interface FileDataIndexSpilledRegionManager<T extends FileDataIndexRegionHelper.Region>
        extends AutoCloseable {
    /**
     * 将索引区域写入磁盘文件。如果目标区域已存在，则覆盖写入。
     *
     * <p>该方法在以下场景被调用：
     * <ul>
     *   <li>从内存缓存中 LRU 淘汰时，将区域溢写到磁盘
     *   <li>区域内容更新时，覆盖磁盘上的旧版本
     * </ul>
     *
     * @param subpartition the subpartition id of this region.
     * @param region the region to be spilled to index file.
     */
    void appendOrOverwriteRegion(int subpartition, T region) throws IOException;

    /**
     * 从磁盘查找包含指定 bufferIndex 的索引区域。
     *
     * <p>查找策略：遍历该子分区的所有 RegionGroup，找到可能包含目标 buffer 的区域。
     *
     * @param subpartition the subpartition id that target region belong to.
     * @param bufferIndex the buffer index that target region contains.
     * @param loadToCache whether to load the found region into the cache.
     * @return if target region can be founded, return it's offset in index file. Otherwise, return
     *     -1.
     */
    long findRegion(int subpartition, int bufferIndex, boolean loadToCache);

    /** 关闭溢写区域管理器，释放文件句柄等资源。 */
    void close() throws IOException;

    /**
     * 溢写区域管理器的工厂接口。
     *
     * @param <T> Region 类型
     */
    interface Factory<T extends FileDataIndexRegionHelper.Region> {
        /**
         * 创建溢写区域管理器实例。
         *
         * @param numSubpartitions 子分区数量
         * @param indexFilePath 索引文件路径
         * @param cacheRegionConsumer 区域加载到缓存的回调，参数为 (子分区ID, 区域)
         */
        FileDataIndexSpilledRegionManager<T> create(
                int numSubpartitions,
                Path indexFilePath,
                BiConsumer<Integer, T> cacheRegionConsumer);
    }
}
