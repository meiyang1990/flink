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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava33.com.google.common.cache.RemovalNotification;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A cache layer of hybrid data index. This class encapsulates the logic of the index's put and get,
 * and automatically caches some indexes in memory. When there are too many cached indexes, it is
 * this class's responsibility to decide and eliminate some indexes to disk.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>文件数据索引缓存是混合 Shuffle 架构中的关键组件，用于加速索引查找。它在内存中维护热点索引区域，
 * 当内存不足时自动淘汰冷数据到磁盘。
 *
 * <h2>缓存架构图示</h2>
 *
 * <pre>
 *                     FileDataIndexCache
 *     ┌─────────────────────────────────────────────┐
 *     │  subpartitionFirstBufferIndexRegions        │
 *     │  ┌────────────────────────────────────────┐ │
 *     │  │ SP0: TreeMap<bufferIndex, Region>      │ │
 *     │  │ SP1: TreeMap<bufferIndex, Region>      │ │
 *     │  │ ...                                    │ │
 *     │  └────────────────────────────────────────┘ │
 *     │                                             │
 *     │  internalCache (Guava LRU)                  │
 *     │  ┌────────────────────────────────────────┐ │
 *     │  │ CachedRegionKey → PLACEHOLDER          │ │
 *     │  │ (用于 LRU 淘汰决策)                      │ │
 *     │  └────────────────────────────────────────┘ │
 *     │                    │                        │
 *     │                    ↓ (淘汰时写入磁盘)         │
 *     │  spilledRegionManager                       │
 *     │  ┌────────────────────────────────────────┐ │
 *     │  │ 管理已溢写到磁盘的索引区域                │ │
 *     │  └────────────────────────────────────────┘ │
 *     └─────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>工作流程</h2>
 *
 * <ol>
 *   <li><b>查询</b>：首先在内存缓存中查找，命中则直接返回；未命中则从磁盘加载
 *   <li><b>插入</b>：新索引直接插入内存缓存，同时更新 LRU 访问记录
 *   <li><b>淘汰</b>：当缓存容量超限时，Guava Cache 自动触发 LRU 淘汰，被淘汰的索引写入磁盘
 * </ol>
 *
 * <h2>设计特点</h2>
 *
 * <ul>
 *   <li>使用 Guava Cache 实现 LRU 淘汰策略
 *   <li>每个子分区独立维护一个 TreeMap，支持高效的区间查询
 *   <li>淘汰时的磁盘写入由 spilledRegionManager 处理
 * </ul>
 *
 * @param <T> Region 类型，必须实现 {@link FileDataIndexRegionHelper.Region} 接口
 */
public class FileDataIndexCache<T extends FileDataIndexRegionHelper.Region> {

    // ================== 核心数据结构 ==================

    /**
     * 存储所有在内存中缓存的索引区域。
     *
     * <p>结构说明：
     * <ul>
     *   <li>外层 List 的索引对应子分区 ID
     *   <li>内层 TreeMap 的 key 是区域的首个 bufferIndex，value 是对应的 Region
     *   <li>使用 TreeMap 是为了支持 floorEntry 操作，高效找到包含指定 bufferIndex 的区域
     * </ul>
     *
     * <p>注意：只有缓存在内存中的区域才会被放入此结构。
     */
    private final List<TreeMap<Integer, T>> subpartitionFirstBufferIndexRegions;

    /**
     * 用于辅助缓存淘汰的内部 Guava Cache。
     *
     * <p>设计说明：
     * <ul>
     *   <li>仅维护缓存区域的 key（CachedRegionKey），value 是占位符
     *   <li>利用 Guava Cache 的 LRU 淘汰机制来决定哪些区域应被淘汰
     *   <li>必须与 subpartitionFirstBufferIndexRegions 保持一致（同步添加/删除）
     * </ul>
     */
    private final Cache<CachedRegionKey, Object> internalCache;

    /** 负责将淘汰的索引区域写入磁盘并管理已溢写区域的组件。 */
    private final FileDataIndexSpilledRegionManager<T> spilledRegionManager;

    /** 索引文件的路径，关闭时会删除此文件。 */
    private final Path indexFilePath;

    /**
     * 缓存条目 value 的占位符。
     * 因为缓存仅用于管理区域的淘汰，不需要真实的 Region 作为 value。
     */
    public static final Object PLACEHOLDER = new Object();

    /**
     * 构造函数：初始化索引缓存。
     *
     * <p>初始化流程：
     * <ol>
     *   <li>为每个子分区创建一个 TreeMap 用于存储缓存的索引区域
     *   <li>创建 Guava Cache 并配置 LRU 淘汰监听器
     *   <li>创建溢写区域管理器，并注册区域加载回调
     * </ol>
     *
     * @param numSubpartitions 子分区数量
     * @param indexFilePath 索引文件路径
     * @param numRetainedInMemoryRegionsMax 内存中最大保留的区域数量（LRU 淘汰阈值）
     * @param spilledRegionManagerFactory 溢写区域管理器工厂
     */
    public FileDataIndexCache(
            int numSubpartitions,
            Path indexFilePath,
            long numRetainedInMemoryRegionsMax,
            FileDataIndexSpilledRegionManager.Factory<T> spilledRegionManagerFactory) {
        this.subpartitionFirstBufferIndexRegions = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionFirstBufferIndexRegions.add(new TreeMap<>());
        }
        this.internalCache =
                CacheBuilder.newBuilder()
                        .maximumSize(numRetainedInMemoryRegionsMax)
                        .removalListener(this::handleRemove)
                        .build();
        this.indexFilePath = checkNotNull(indexFilePath);
        this.spilledRegionManager =
                spilledRegionManagerFactory.create(
                        numSubpartitions,
                        indexFilePath,
                        (subpartition, region) -> {
                            if (!getCachedRegionContainsTargetBufferIndex(
                                            subpartition, region.getFirstBufferIndex())
                                    .isPresent()) {
                                subpartitionFirstBufferIndexRegions
                                        .get(subpartition)
                                        .put(region.getFirstBufferIndex(), region);
                                internalCache.put(
                                        new CachedRegionKey(
                                                subpartition, region.getFirstBufferIndex()),
                                        PLACEHOLDER);
                            } else {
                                // this is needed for cache entry remove algorithm like LRU.
                                internalCache.getIfPresent(
                                        new CachedRegionKey(
                                                subpartition, region.getFirstBufferIndex()));
                            }
                        });
    }

    /**
     * 获取包含目标 bufferIndex 的索引区域。
     *
     * <p>查询流程：
     * <ol>
     *   <li>首先在内存缓存中查找包含目标 bufferIndex 的区域
     *   <li>如果命中，更新 LRU 访问记录并返回
     *   <li>如果未命中，委托 spilledRegionManager 从磁盘查找并加载到缓存
     *   <li>再次从内存缓存中获取并返回
     * </ol>
     *
     * @param subpartitionId the subpartition that target buffer belong to.
     * @param bufferIndex the index of target buffer.
     * @return If target region can be founded from memory or disk, return optional contains target
     *     region. Otherwise, return {@code Optional#empty()};
     */
    public Optional<T> get(int subpartitionId, int bufferIndex) {
        // 首先尝试从内存缓存中获取区域
        Optional<T> regionOpt =
                getCachedRegionContainsTargetBufferIndex(subpartitionId, bufferIndex);
        if (regionOpt.isPresent()) {
            T region = regionOpt.get();
            checkNotNull(
                    // 访问 internalCache 以更新 LRU 记录，防止热点区域被淘汰
                    internalCache.getIfPresent(
                            new CachedRegionKey(subpartitionId, region.getFirstBufferIndex())));
            return Optional.of(region);
        } else {
            // 内存未命中，尝试从磁盘查找目标区域并加载到缓存
            spilledRegionManager.findRegion(subpartitionId, bufferIndex, true);
            return getCachedRegionContainsTargetBufferIndex(subpartitionId, bufferIndex);
        }
    }

    /**
     * 将索引区域批量放入缓存。
     *
     * <p>该方法会将指定子分区的多个区域一次性放入内存缓存中，
     * 同时更新 internalCache 中的 LRU 记录。
     *
     * @param subpartition the subpartition's id of regions.
     * @param fileRegions regions to be cached.
     */
    public void put(int subpartition, List<T> fileRegions) {
        TreeMap<Integer, T> treeMap = subpartitionFirstBufferIndexRegions.get(subpartition);
        for (T region : fileRegions) {
            internalCache.put(
                    new CachedRegionKey(subpartition, region.getFirstBufferIndex()), PLACEHOLDER);
            treeMap.put(region.getFirstBufferIndex(), region);
        }
    }

    /**
     * 关闭索引缓存。
     *
     * <p>关闭时会删除索引文件，之后索引无法再读取或写入。
     */
    public void close() throws IOException {
        spilledRegionManager.close();
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    /**
     * Guava Cache 淘汰回调：当区域被 LRU 策略淘汰时触发。
     *
     * <p>处理流程：
     * <ol>
     *   <li>从内存缓存中移除被淘汰的区域
     *   <li>将被淘汰的区域写入磁盘，由 GC 负责后续回收
     * </ol>
     */
    private void handleRemove(RemovalNotification<CachedRegionKey, Object> removedEntry) {
        CachedRegionKey removedKey = removedEntry.getKey();
        // 从内存缓存中移除被淘汰的区域
        T removedRegion =
                subpartitionFirstBufferIndexRegions
                        .get(removedKey.getSubpartition())
                        .remove(removedKey.getFirstBufferIndex());

        // 将区域写入磁盘文件。写入后该区域不再有强引用指向，可被 GC 安全回收
        writeRegion(removedKey.getSubpartition(), removedRegion);
    }

    /** 将单个区域写入磁盘索引文件。 */
    private void writeRegion(int subpartition, T region) {
        try {
            spilledRegionManager.appendOrOverwriteRegion(subpartition, region);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * 从内存缓存中查找包含目标 bufferIndex 的区域。
     *
     * <p>查找策略：使用 TreeMap 的 floorEntry 方法找到 firstBufferIndex 小于等于目标的区域，
     * 然后检查该区域是否真正包含目标 bufferIndex。
     *
     * @param subpartitionId the subpartition that target buffer belong to.
     * @param bufferIndex the index of target buffer.
     * @return If target region is cached in memory, return optional contains target region.
     *     Otherwise, return {@code Optional#empty()};
     */
    private Optional<T> getCachedRegionContainsTargetBufferIndex(
            int subpartitionId, int bufferIndex) {
        return Optional.ofNullable(
                        subpartitionFirstBufferIndexRegions
                                .get(subpartitionId)
                                // 找到 firstBufferIndex <= bufferIndex 的最大 entry
                                .floorEntry(bufferIndex))
                .map(Map.Entry::getValue)
                // 验证该区域确实包含目标 bufferIndex
                .filter(internalRegion -> internalRegion.containBuffer(bufferIndex));
    }

    /**
     * 缓存区域的唯一标识 key。
     *
     * <p>由子分区 ID 和区域的 firstBufferIndex 共同唯一标识一个缓存区域。
     * 用于在 Guava Cache 中作为 key 进行 LRU 淘汰管理。
     */
    private static class CachedRegionKey {
        /** 缓存区域所属的子分区 ID。 */
        private final int subpartition;

        /** 缓存区域的首个 buffer 索引。 */
        private final int firstBufferIndex;

        public CachedRegionKey(int subpartition, int firstBufferIndex) {
            this.subpartition = subpartition;
            this.firstBufferIndex = firstBufferIndex;
        }

        public int getSubpartition() {
            return subpartition;
        }

        public int getFirstBufferIndex() {
            return firstBufferIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CachedRegionKey that = (CachedRegionKey) o;
            return subpartition == that.subpartition && firstBufferIndex == that.firstBufferIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subpartition, firstBufferIndex);
        }
    }
}
