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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * Default implementation of {@link FileDataIndexSpilledRegionManager}. This manager will handle and
 * spill regions in the following way:
 *
 * <ul>
 *   <li>All regions will be written to the same file, namely index file.
 *   <li>Multiple regions belonging to the same subpartition form a region group.
 *   <li>The regions in the same region group have no special relationship, but are only related to
 *       the order in which they are spilled.
 *   <li>Each region group is independent. Even if the previous region group is not full, the next
 *       region group can still be allocated.
 *   <li>If a region has been written to the index file already, spill it again will overwrite the
 *       previous region.
 *   <li>The very large region will monopolize a single region group.
 * </ul>
 *
 * <p>The relationships between index file and region group are shown below.
 *
 * <pre>
 *
 *         - - - - - - - - - Index File - - — - - - - - - - - -
 *        |                                                     |
 *        | - - — -RegionGroup1 - -   - - RegionGroup2- - - -   |
 *        ||SP1 R1｜｜SP1 R2｜ Free | |SP2 R3| SP2 R1| SP2 R2 |  |
 *        | - - - - - - - - - - - -   - - - - - - - - - - - -   |
 *        |                                                     |
 *        | - - - - - - - -RegionGroup3 - - - - -               |
 *        ||              Big Region             |              |
 *        | - - - - - - - - - - - - - - - - - - -               |
 *         - - - - - - - - - - - - - - - - - - - - - -- - - - -
 * </pre>
 *
 * <h2>核心设计概述</h2>
 *
 * <p>该实现采用 <b>Region Group</b> 的概念来组织磁盘上的索引区域：
 *
 * <ul>
 *   <li>每个子分区有自己独立的一系列 Region Group
 *   <li>每个 Region Group 有固定的大小限制（regionGroupSizeInBytes）
 *   <li>当 Region Group 空间不足时，分配新的 Group
 *   <li>超大 Region 独占一个 Region Group
 * </ul>
 *
 * <h2>查找优化</h2>
 *
 * <p>查找时会根据 bufferIndex 范围快速定位可能包含目标的 Region Group，
 * 避免全量扫描索引文件。
 *
 * <h2>缓存加载策略</h2>
 *
 * <p>支持两种缓存加载策略：
 * <ul>
 *   <li>仅加载目标 Region（缓存容量小时）
 *   <li>加载整个 Region Group（缓存容量大时，提高缓存命中率）
 * </ul>
 *
 * @param <T> Region 类型
 */
public class FileDataIndexSpilledRegionManagerImpl<T extends FileDataIndexRegionHelper.Region>
        implements FileDataIndexSpilledRegionManager<T> {

    // ================== 元数据管理 ==================

    /**
     * 每个子分区已完成的 Region Group 元数据列表。
     *
     * <p>结构说明：
     * <ul>
     *   <li>外层 List 的索引对应子分区 ID
     *   <li>内层 TreeMap 的 key 是 Region Group 的 minBufferIndex，value 是 RegionGroup 元数据
     *   <li>只有已完成（不再追加）的 Region Group 才会放入此结构
     * </ul>
     */
    private final List<TreeMap<Integer, RegionGroup>> subpartitionFinishedRegionGroupMetas;

    // ================== 文件 I/O ==================

    /** 索引文件的文件通道，用于读写索引数据。 */
    private FileChannel channel;

    /** 下一个 Region Group 的起始偏移，新分配的 Region Group 从此位置开始。 */
    private long nextRegionGroupOffset = 0L;

    // ================== 每子分区状态 ==================

    /** 每个子分区当前的写入偏移量。 */
    private final long[] subpartitionCurrentOffset;

    /** 每个子分区当前 Region Group 的剩余空间（字节数）。 */
    private final int[] subpartitionFreeSpaceInBytes;

    /** 每个子分区当前正在写入的 Region Group 元数据。 */
    private final RegionGroup[] currentRegionGroup;

    // ================== 配置参数 ==================

    /**
     * Region Group 的默认大小。
     * 如果单个 Region 的大小超过此值，该 Region 将独占一个 Region Group。
     */
    private final int regionGroupSizeInBytes;

    /**
     * 区域加载到缓存的回调函数。
     * 第一个参数是子分区 ID，第二个参数是要加载的 Region。
     */
    private final BiConsumer<Integer, T> cacheRegionConsumer;

    /** 用于读写 Region 的辅助类。 */
    private final FileDataIndexRegionHelper<T> fileDataIndexRegionHelper;

    /**
     * 是否将整个 Region Group 加载到缓存。
     * 当缓存容量足够大时启用，可以提高缓存命中率。
     */
    private final boolean loadEntireRegionGroupToCache;

    public FileDataIndexSpilledRegionManagerImpl(
            int numSubpartitions,
            Path indexFilePath,
            int regionGroupSizeInBytes,
            long maxCacheCapacity,
            int regionHeaderSize,
            BiConsumer<Integer, T> cacheRegionConsumer,
            FileDataIndexRegionHelper<T> fileDataIndexRegionHelper) {
        try {
            this.channel =
                    FileChannel.open(
                            indexFilePath,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        this.loadEntireRegionGroupToCache =
                shouldLoadEntireRegionGroupToCache(
                        numSubpartitions,
                        regionGroupSizeInBytes,
                        maxCacheCapacity,
                        regionHeaderSize);
        this.subpartitionFinishedRegionGroupMetas = new ArrayList<>(numSubpartitions);
        this.subpartitionCurrentOffset = new long[numSubpartitions];
        this.subpartitionFreeSpaceInBytes = new int[numSubpartitions];
        this.currentRegionGroup = new RegionGroup[numSubpartitions];
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionFinishedRegionGroupMetas.add(new TreeMap<>());
        }
        this.cacheRegionConsumer = cacheRegionConsumer;
        this.fileDataIndexRegionHelper = fileDataIndexRegionHelper;
        this.regionGroupSizeInBytes = regionGroupSizeInBytes;
    }

    /**
     * 从磁盘查找包含指定 bufferIndex 的索引区域。
     *
     * <p>查找流程：
     * <ol>
     *   <li>首先在当前正在写入的 Region Group 中查找
     *   <li>如果未找到，在已完成的 Region Group 中按 minBufferIndex 范围查找
     *   <li>找到后可选择性地加载到内存缓存
     * </ol>
     *
     * @return 如果找到目标 Region，返回其在文件中的偏移量；否则返回 -1
     */
    @Override
    public long findRegion(int subpartition, int bufferIndex, boolean loadToCache) {
        // 首先在当前正在写入的 Region Group 中查找
        RegionGroup regionGroup = currentRegionGroup[subpartition];
        if (regionGroup != null) {
            long regionOffset =
                    findRegionInRegionGroup(subpartition, bufferIndex, regionGroup, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }

        // 在已完成的 Region Group 中查找
        TreeMap<Integer, RegionGroup> subpartitionRegionGroupMetaTreeMap =
                subpartitionFinishedRegionGroupMetas.get(subpartition);
        // 所有 minBufferIndex 小于等于目标 bufferIndex 的 Region Group 都可能包含目标 Region
        for (RegionGroup meta :
                subpartitionRegionGroupMetaTreeMap.headMap(bufferIndex, true).values()) {
            long regionOffset =
                    findRegionInRegionGroup(subpartition, bufferIndex, meta, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }
        return -1;
    }

    private long findRegionInRegionGroup(
            int subpartition, int bufferIndex, RegionGroup meta, boolean loadToCache) {
        if (bufferIndex <= meta.getMaxBufferIndex()) {
            try {
                return readRegionGroupAndLoadToCacheIfNeeded(
                        subpartition, bufferIndex, meta, loadToCache);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // -1 表示在此 Region Group 中未找到目标 Region
        return -1;
    }

    /**
     * 在 Region Group 内读取并加载 Region 到缓存。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>从磁盘读取整个 Region Group 的所有 Region
     *   <li>遍历找到目标 Region
     *   <li>根据配置决定加载策略（仅目标/整个 Group）
     * </ol>
     */
    private long readRegionGroupAndLoadToCacheIfNeeded(
            int subpartition, int bufferIndex, RegionGroup meta, boolean loadToCache)
            throws IOException {
        // 读取该 Region Group 的所有 Region
        List<Tuple2<T, Long>> regionAndOffsets =
                readRegionGroup(meta.getOffset(), meta.getNumRegions());
        // -1 表示在此 Region Group 中未找到目标 Region
        long targetRegionOffset = -1;
        T targetRegion = null;
        // 遍历所有 Region 查找目标
        Iterator<Tuple2<T, Long>> it = regionAndOffsets.iterator();
        while (it.hasNext()) {
            Tuple2<T, Long> regionAndOffset = it.next();
            T region = regionAndOffset.f0;
            // 检查该 Region 是否包含目标 buffer
            if (region.containBuffer(bufferIndex)) {
                // 找到目标 Region
                targetRegion = region;
                targetRegionOffset = regionAndOffset.f1;
                it.remove();
            }
        }

        // 找到目标 Region 且需要加载到缓存
        if (targetRegion != null && loadToCache) {
            if (loadEntireRegionGroupToCache) {
                // 先加载目标之外的所有 Region 到缓存
                regionAndOffsets.forEach(
                        (regionAndOffsetTuple) ->
                                cacheRegionConsumer.accept(subpartition, regionAndOffsetTuple.f0));
                // 最后加载目标 Region，以防止目标被 LRU 淘汰
                cacheRegionConsumer.accept(subpartition, targetRegion);
            } else {
                // 仅加载目标 Region 到缓存
                cacheRegionConsumer.accept(subpartition, targetRegion);
            }
        }
        // 返回目标 Region 的文件偏移量
        return targetRegionOffset;
    }

    /**
     * 将 Region 写入索引文件。如果已存在则覆盖，否则追加。
     *
     * <p><b>注意</b>：此方法在淘汰 Region 时被调用，不能将 Region 重新加载到缓存，
     * 否则会导致无限循环。
     */
    @Override
    public void appendOrOverwriteRegion(int subpartition, T newRegion) throws IOException {
        // 该方法仅在需要淘汰 Region 时被调用，不能让 Region 被重新加载到缓存，否则会无限循环
        long oldRegionOffset = findRegion(subpartition, newRegion.getFirstBufferIndex(), false);
        if (oldRegionOffset != -1) {
            // 如果 Region 已存在于文件中，直接覆盖写入
            writeRegionToOffset(oldRegionOffset, newRegion);
        } else {
            // 否则追加 Region 到 Region Group
            appendRegion(subpartition, newRegion);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    /**
     * 判断是否应该将整个 Region Group 加载到缓存。
     *
     * <p>决策逻辑：如果缓存能容纳每个子分区至少两个 Region Group（一个用于读取，一个用于写入），
     * 则整个 Group 加载是合理的，可以提高缓存命中率。否则只加载目标 Region。
     */
    private static boolean shouldLoadEntireRegionGroupToCache(
            int numSubpartitions,
            int regionGroupSizeInBytes,
            long maxCacheCapacity,
            int regionHeaderSize) {
        // If the cache can put at least two region groups (one for reading and one for writing) for
        // each subpartition, it is reasonable to load the entire region group into memory, which
        // can improve the cache hit rate. On the contrary, if the cache capacity is small, loading
        // a large number of regions will lead to performance degradation,only the target region
        // should be loaded.
        return ((long) 2 * numSubpartitions * regionGroupSizeInBytes) / regionHeaderSize
                <= maxCacheCapacity;
    }

    /**
     * 追加 Region 到当前 Region Group。
     *
     * <p>如果当前 Region Group 空间不足，会先分配新的 Region Group。
     * 超大 Region 会独占一个 Region Group。
     */
    private void appendRegion(int subpartition, T region) throws IOException {
        int regionSize = region.getSize();
        // 检查当前 Region Group 是否有足够空间
        if (subpartitionFreeSpaceInBytes[subpartition] < regionSize) {
            // 空间不足，分配新的 Region Group。如果 Region 超大，则独占一个 Group
            startNewRegionGroup(subpartition, Math.max(regionSize, regionGroupSizeInBytes));
        }
        // 将 Region 写入当前偏移位置
        writeRegionToOffset(subpartitionCurrentOffset[subpartition], region);
        // 更新 Region Group 元数据
        updateRegionGroup(subpartition, region);
    }

    /** 将 Region 写入指定的文件偏移位置。 */
    private void writeRegionToOffset(long offset, T region) throws IOException {
        channel.position(offset);
        fileDataIndexRegionHelper.writeRegionToFile(channel, region);
    }

    /**
     * 为指定子分区分配新的 Region Group。
     *
     * <p>处理流程：
     * <ol>
     *   <li>将当前 Region Group（如果有）标记为已完成并加入元数据列表
     *   <li>创建新的 Region Group 并更新相关状态
     * </ol>
     */
    private void startNewRegionGroup(int subpartition, int newRegionGroupSize) {
        RegionGroup oldRegionGroup = currentRegionGroup[subpartition];
        // 创建新的 Region Group
        currentRegionGroup[subpartition] = new RegionGroup(nextRegionGroupOffset);
        subpartitionCurrentOffset[subpartition] = nextRegionGroupOffset;
        // 更新下一个 Region Group 的起始偏移
        nextRegionGroupOffset += newRegionGroupSize;
        subpartitionFreeSpaceInBytes[subpartition] = newRegionGroupSize;
        if (oldRegionGroup != null) {
            // 将旧的 Region Group 加入已完成列表
            subpartitionFinishedRegionGroupMetas
                    .get(subpartition)
                    .put(oldRegionGroup.minBufferIndex, oldRegionGroup);
        }
    }

    /** 更新 Region Group 元数据：减少剩余空间，推进偏移量，更新 buffer index 范围。 */
    private void updateRegionGroup(int subpartition, T region) {
        int regionSize = region.getSize();
        subpartitionFreeSpaceInBytes[subpartition] -= regionSize;
        subpartitionCurrentOffset[subpartition] += regionSize;
        RegionGroup regionGroup = currentRegionGroup[subpartition];
        regionGroup.addRegion(
                region.getFirstBufferIndex(),
                region.getFirstBufferIndex() + region.getNumBuffers() - 1);
    }

    /**
     * 从索引文件读取一个 Region Group 的所有 Region。
     *
     * @param offset Region Group 在文件中的起始偏移
     * @param numRegions Region Group 包含的 Region 数量
     * @return Region 列表，每个元素包含 Region 及其文件偏移
     */
    private List<Tuple2<T, Long>> readRegionGroup(long offset, int numRegions) throws IOException {
        List<Tuple2<T, Long>> regionAndOffsets = new ArrayList<>();
        for (int i = 0; i < numRegions; i++) {
            T region = fileDataIndexRegionHelper.readRegionFromFile(channel, offset);
            regionAndOffsets.add(Tuple2.of(region, offset));
            offset += region.getSize();
        }
        return regionAndOffsets;
    }

    /**
     * Region Group 元数据。
     *
     * <p>用于跟踪一组已溢写到磁盘的 Region 的元信息。
     * 当 Region Group 完成（不再追加新 Region）后，其元数据变为不可变。
     *
     * <h3>元数据字段</h3>
     * <ul>
     *   <li><b>minBufferIndex</b>：该 Group 中所有 Region 的最小 firstBufferIndex
     *   <li><b>maxBufferIndex</b>：该 Group 中所有 Region 的最大 lastBufferIndex
     *   <li><b>numRegions</b>：该 Group 包含的 Region 数量
     *   <li><b>offset</b>：该 Group 在索引文件中的起始偏移
     * </ul>
     */
    private static class RegionGroup {
        /** 该 Region Group 的最小 buffer index（所有 Region 中的最小 firstBufferIndex）。 */
        private int minBufferIndex;

        /** 该 Region Group 的最大 buffer index（所有 Region 中的最大 lastBufferIndex）。 */
        private int maxBufferIndex;

        /** 该 Region Group 包含的 Region 数量。 */
        private int numRegions;

        /** 该 Region Group 在索引文件中的起始偏移。 */
        private final long offset;

        public RegionGroup(long offset) {
            this.offset = offset;
            this.minBufferIndex = Integer.MAX_VALUE;
            this.maxBufferIndex = 0;
            this.numRegions = 0;
        }

        public int getMaxBufferIndex() {
            return maxBufferIndex;
        }

        public long getOffset() {
            return offset;
        }

        public int getNumRegions() {
            return numRegions;
        }

        /**
         * 将一个新 Region 添加到此 Region Group，更新元数据。
         *
         * @param firstBufferIndexOfRegion 新 Region 的首个 buffer index
         * @param maxBufferIndexOfRegion 新 Region 的最后一个 buffer index
         */
        public void addRegion(int firstBufferIndexOfRegion, int maxBufferIndexOfRegion) {
            // 更新最小 buffer index
            if (firstBufferIndexOfRegion < minBufferIndex) {
                this.minBufferIndex = firstBufferIndexOfRegion;
            }
            // 更新最大 buffer index
            if (maxBufferIndexOfRegion > maxBufferIndex) {
                this.maxBufferIndex = maxBufferIndexOfRegion;
            }
            this.numRegions++;
        }
    }

    /**
     * {@link FileDataIndexSpilledRegionManager} 的工厂类。
     *
     * <p>封装创建溢写区域管理器所需的配置参数。
     *
     * @param <T> Region 类型
     */
    public static class Factory<T extends FileDataIndexRegionHelper.Region>
            implements FileDataIndexSpilledRegionManager.Factory<T> {
        /** Region Group 的默认大小（字节）。 */
        private final int regionGroupSizeInBytes;

        /** 缓存的最大容量（Region 数量）。 */
        private final long maxCacheCapacity;

        /** Region 头部的固定大小（字节）。 */
        private final int regionHeaderSize;

        /** Region 读写辅助类。 */
        private final FileDataIndexRegionHelper<T> fileDataIndexRegionHelper;

        public Factory(
                int regionGroupSizeInBytes,
                long maxCacheCapacity,
                int regionHeaderSize,
                FileDataIndexRegionHelper<T> fileDataIndexRegionHelper) {
            this.regionGroupSizeInBytes = regionGroupSizeInBytes;
            this.maxCacheCapacity = maxCacheCapacity;
            this.regionHeaderSize = regionHeaderSize;
            this.fileDataIndexRegionHelper = fileDataIndexRegionHelper;
        }

        @Override
        public FileDataIndexSpilledRegionManager<T> create(
                int numSubpartitions,
                Path indexFilePath,
                BiConsumer<Integer, T> cacheRegionConsumer) {
            return new FileDataIndexSpilledRegionManagerImpl<>(
                    numSubpartitions,
                    indexFilePath,
                    regionGroupSizeInBytes,
                    maxCacheCapacity,
                    regionHeaderSize,
                    cacheRegionConsumer,
                    fileDataIndexRegionHelper);
        }
    }
}
