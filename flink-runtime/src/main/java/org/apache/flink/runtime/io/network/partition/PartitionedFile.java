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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link PartitionedFile} is the persistent file type of sort-merge based blocking shuffle. Each
 * {@link PartitionedFile} contains two physical files: one is the data file and the other is the
 * index file. Both the data file and the index file have multiple regions. Data belonging to the
 * same subpartition are stored together in each data region and the corresponding index region
 * contains index entries of all subpartitions. Each index entry is a (long, integer) value tuple of
 * which the long value represents the file offset of the target subpartition and the integer value
 * is the number of buffers.
 *
 * <p>Sort-Merge Blocking Shuffle 的持久化文件类型。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>双文件结构</b>：每个 PartitionedFile 包含数据文件(.shuffle.data)和索引文件(.shuffle.index)</li>
 *   <li><b>多 Region 组织</b>：数据文件和索引文件都由多个 Region 组成，每次 flush 产生一个新 Region</li>
 *   <li><b>子分区数据连续存储</b>：同一子分区的数据在每个 Region 内连续存放，便于顺序读取</li>
 *   <li><b>索引加速</b>：支持将索引数据缓存到内存，减少磁盘 I/O</li>
 * </ul>
 *
 * <h2>文件布局</h2>
 * <pre>
 * 数据文件 (.shuffle.data):
 * +------------------+------------------+-----+------------------+
 * |    Region 0      |    Region 1      | ... |    Region N      |
 * +------------------+------------------+-----+------------------+
 * 每个 Region 内部:
 * +------------+------------+-----+------------+
 * | Subpart 0  | Subpart 1  | ... | Subpart M  |
 * +------------+------------+-----+------------+
 *
 * 索引文件 (.shuffle.index):
 * +----------------+----------------+-----+----------------+
 * | Region 0 Index | Region 1 Index | ... | Region N Index |
 * +----------------+----------------+-----+----------------+
 * 每个 Region 的索引:
 * +----------------+----------------+-----+----------------+
 * | Entry(SP0)     | Entry(SP1)     | ... | Entry(SPM)     |
 * +----------------+----------------+-----+----------------+
 * 每个 Entry = 8 bytes offset + 8 bytes size = 16 bytes
 * </pre>
 *
 * <h2>与其他组件的关系</h2>
 * <ul>
 *   <li>{@link PartitionedFileWriter}：负责创建和写入 PartitionedFile</li>
 *   <li>{@link PartitionedFileReader}：负责读取 PartitionedFile 中的数据</li>
 *   <li>{@link SortMergeResultPartition}：使用 PartitionedFile 作为数据存储介质</li>
 * </ul>
 */
public class PartitionedFile {

    /** 数据文件后缀名，包含所有 Shuffle 数据 */
    public static final String DATA_FILE_SUFFIX = ".shuffle.data";

    /** 索引文件后缀名，包含所有子分区在数据文件中的偏移和大小信息 */
    public static final String INDEX_FILE_SUFFIX = ".shuffle.index";

    /**
     * Size of each index entry in the index file: 8 bytes for file offset and 8 bytes for data size
     * in bytes.
     *
     * <p>每个索引条目的大小：
     * <ul>
     *   <li>8 字节：子分区数据在数据文件中的起始偏移量</li>
     *   <li>8 字节：子分区数据的字节数</li>
     * </ul>
     */
    public static final int INDEX_ENTRY_SIZE = 8 + 8;

    /**
     * Number of data regions in this {@link PartitionedFile}.
     *
     * <p>数据区域数量。每次 {@link PartitionedFileWriter#startNewRegion} 调用会创建一个新的 Region，
     * 通常对应一次 DataBuffer 的 flush 操作
     */
    private final int numRegions;

    /**
     * Number of subpartitions of this {@link PartitionedFile}.
     *
     * <p>子分区数量，对应下游消费者数量
     */
    private final int numSubpartitions;

    /**
     * Path of the data file which stores all data in this {@link PartitionedFile}.
     *
     * <p>数据文件路径，格式：{basePath}.shuffle.data
     */
    private final Path dataFilePath;

    /**
     * Path of the index file which stores all index entries in this {@link PartitionedFile}.
     *
     * <p>索引文件路径，格式：{basePath}.shuffle.index
     */
    private final Path indexFilePath;

    /** 数据文件总大小（字节） */
    private final long dataFileSize;

    /** 索引文件总大小（字节），等于 numRegions * numSubpartitions * INDEX_ENTRY_SIZE */
    private final long indexFileSize;

    /** 数据文件中 Buffer 的总数量，用于统计监控 */
    private final long numBuffers;

    /**
     * Used to accelerate index data access.
     *
     * <p>索引数据缓存，用于加速索引访问：
     * <ul>
     *   <li>当索引文件较小时，所有索引数据会被缓存到此 ByteBuffer 中</li>
     *   <li>读取索引时优先从缓存读取，避免磁盘 I/O</li>
     *   <li>如果索引文件过大超过配置的最大缓存大小，则为 null，每次读取需要访问磁盘</li>
     * </ul>
     */
    @Nullable private final ByteBuffer indexEntryCache;

    public PartitionedFile(
            int numRegions,
            int numSubpartitions,
            Path dataFilePath,
            Path indexFilePath,
            long dataFileSize,
            long indexFileSize,
            long numBuffers,
            @Nullable ByteBuffer indexEntryCache) {
        checkArgument(numRegions >= 0, "Illegal number of data regions.");
        checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");

        this.numRegions = numRegions;
        this.numSubpartitions = numSubpartitions;
        this.dataFilePath = checkNotNull(dataFilePath);
        this.indexFilePath = checkNotNull(indexFilePath);
        this.dataFileSize = dataFileSize;
        this.indexFileSize = indexFileSize;
        this.numBuffers = numBuffers;
        this.indexEntryCache = indexEntryCache;
    }

    public Path getDataFilePath() {
        return dataFilePath;
    }

    public Path getIndexFilePath() {
        return indexFilePath;
    }

    public int getNumRegions() {
        return numRegions;
    }

    /** 检查数据文件和索引文件是否都可读 */
    public boolean isReadable() {
        return Files.isReadable(dataFilePath) && Files.isReadable(indexFilePath);
    }

    /**
     * Returns the index entry offset of the target region and subpartition in the index file. Both
     * region index and subpartition index start from 0.
     *
     * <p>计算指定 Region 和子分区在索引文件中的字节偏移量。
     * 偏移量计算公式：(region * numSubpartitions + subpartition) * INDEX_ENTRY_SIZE
     */
    private long getIndexEntryOffset(int region, int subpartition) {
        checkArgument(region >= 0 && region < getNumRegions(), "Illegal target region.");
        checkArgument(
                subpartition >= 0 && subpartition < numSubpartitions,
                "Subpartition index out of bound.");

        return (((long) region) * numSubpartitions + subpartition) * INDEX_ENTRY_SIZE;
    }

    /**
     * Gets the index entry of the target region and subpartition either from the index data cache
     * or the index data file.
     *
     * <p>获取指定 Region 和子分区的索引条目，读取策略：
     * <ol>
     *   <li>优先从内存缓存 {@link #indexEntryCache} 读取</li>
     *   <li>缓存不可用时，从索引文件中定位并读取</li>
     * </ol>
     *
     * <p>读取完成后，target ByteBuffer 的位置被重置为 0（flip），可直接读取索引内容
     *
     * @param indexFile 索引文件通道
     * @param target 目标缓冲区，容量必须等于 INDEX_ENTRY_SIZE
     * @param region 目标 Region 索引
     * @param subpartition 目标子分区索引
     */
    void getIndexEntry(FileChannel indexFile, ByteBuffer target, int region, int subpartition)
            throws IOException {
        checkArgument(target.capacity() == INDEX_ENTRY_SIZE, "Illegal target buffer size.");

        target.clear();
        long indexEntryOffset = getIndexEntryOffset(region, subpartition);
        // 优先从内存缓存读取索引数据
        if (indexEntryCache != null) {
            for (int i = 0; i < INDEX_ENTRY_SIZE; ++i) {
                target.put(indexEntryCache.get((int) indexEntryOffset + i));
            }
        } else {
            // 缓存不可用，从文件读取（需要加锁避免并发定位问题）
            synchronized (indexFilePath) {
                indexFile.position(indexEntryOffset);
                BufferReaderWriterUtil.readByteBufferFully(indexFile, target);
            }
        }
        target.flip();
    }

    /** 静默删除数据文件和索引文件，删除失败不抛异常 */
    public void deleteQuietly() {
        IOUtils.deleteFileQuietly(dataFilePath);
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    @Override
    public String toString() {
        return "PartitionedFile{"
                + "numRegions="
                + numRegions
                + ", numSubpartitions="
                + numSubpartitions
                + ", dataFilePath="
                + dataFilePath
                + ", indexFilePath="
                + indexFilePath
                + ", dataFileSize="
                + dataFileSize
                + ", indexFileSize="
                + indexFileSize
                + ", numBuffers="
                + numBuffers
                + ", indexDataCached="
                + (indexEntryCache != null)
                + '}';
    }
}
