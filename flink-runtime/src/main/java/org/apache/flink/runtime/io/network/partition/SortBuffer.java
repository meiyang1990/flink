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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DataBuffer} implementation which sorts all appended records only by subpartition index.
 * Records of the same subpartition keep the appended order.
 *
 * <p>It maintains a list of {@link MemorySegment}s as a joint buffer. Data will be appended to the
 * joint buffer sequentially. When writing a record, an index entry will be appended first. An index
 * entry consists of 4 fields: 4 bytes for record length, 4 bytes for {@link Buffer.DataType} and 8
 * bytes for address pointing to the next index entry of the same subpartition which will be used to
 * index the next record to read when coping data from this {@link DataBuffer}. For simplicity, no
 * index entry can span multiple segments. The corresponding record data is seated right after its
 * index entry and different from the index entry, records have variable length thus may span
 * multiple segments.
 *
 * <h2>核心设计概述</h2>
 * <p>SortBuffer 是 {@link DataBuffer} 的一种抽象实现，采用"索引链表 + 连续数据"的方式组织数据，
 * 所有子分区的数据物理上连续存储，通过索引链表实现按子分区的逻辑组织。
 *
 * <h3>1. 内存布局结构</h3>
 * <pre>
 *   Joint Buffer 内存布局（多个 MemorySegment 逻辑连续）：
 *   +----------------+----------------+----------------+----------------+
 *   | Index Entry 1  | Record Data 1  | Index Entry 2  | Record Data 2  |
 *   | (SP0, 100B)    | [100 bytes]    | (SP1, 50B)     | [50 bytes]     |
 *   +----------------+----------------+----------------+----------------+
 *   | Index Entry 3  | Record Data 3  | Index Entry 4  | ...            |
 *   | (SP0, 200B)    | [200 bytes]    | (SP2, 80B)     |                |
 *   +----------------+----------------+----------------+----------------+
 *
 *   索引项结构（16 字节）：
 *   +------------------+------------------+---------------------------+
 *   | Record Length    | Data Type        | Next Index Entry Address  |
 *   | (4 bytes)        | (4 bytes)        | (8 bytes: seg_idx + off)  |
 *   +------------------+------------------+---------------------------+
 *
 *   索引链表示例（SP0 的记录链）：
 *   firstIndexEntryAddresses[0] --> Entry1 --> Entry3 --> Entry7 --> ...
 *                                     ↓          ↓          ↓
 *                                   Data1      Data3      Data7
 * </pre>
 *
 * <h3>2. 关键设计决策</h3>
 * <ul>
 *   <li><b>索引项不跨段</b>：简化解析逻辑，索引项必须完整存放在单个 MemorySegment 中</li>
 *   <li><b>数据可跨段</b>：变长记录数据允许跨越多个 MemorySegment</li>
 *   <li><b>地址编码</b>：使用 64 位长整型编码地址，高 32 位为段索引，低 32 位为段内偏移</li>
 *   <li><b>原子写入</b>：整条记录要么全部写入成功，要么不写入（与 HashBasedDataBuffer 不同）</li>
 * </ul>
 *
 * <h3>3. 与 HashBasedDataBuffer 的对比</h3>
 * <table border="1">
 *   <tr><th>特性</th><th>SortBuffer</th><th>HashBasedDataBuffer</th></tr>
 *   <tr><td>数据组织</td><td>混合存储 + 索引链表</td><td>按子分区隔离存储</td></tr>
 *   <tr><td>内存效率</td><td>高（无碎片）</td><td>可能有内部碎片</td></tr>
 *   <tr><td>写入方式</td><td>原子写入</td><td>支持部分写入</td></tr>
 *   <tr><td>读取复杂度</td><td>需要数据拷贝</td><td>直接返回 Buffer</td></tr>
 * </table>
 *
 * <h3>4. 子类</h3>
 * <ul>
 *   <li>{@link SortBasedDataBuffer}：提供 getNextBuffer 的具体实现，用于 SortMergeResultPartition</li>
 * </ul>
 *
 * @see SortBasedDataBuffer
 * @see HashBasedDataBuffer
 * @see SortMergeResultPartition
 */
@NotThreadSafe
public abstract class SortBuffer implements DataBuffer {

    /**
     * Size of an index entry: 4 bytes for record length, 4 bytes for data type and 8 bytes for
     * pointer to next entry.
     *
     * <p>索引项大小（16 字节）：
     * <ul>
     *   <li>4 字节：记录长度（存储在 64 位长整型的高 32 位）</li>
     *   <li>4 字节：数据类型（存储在 64 位长整型的低 32 位）</li>
     *   <li>8 字节：下一个索引项的地址指针（同子分区的链表链接）</li>
     * </ul>
     */
    protected static final int INDEX_ENTRY_SIZE = 4 + 4 + 8;

    // =============================================================================================
    // 内存管理相关字段
    // =============================================================================================

    /**
     * A list of {@link MemorySegment}s used to store data in memory.
     *
     * <p>空闲内存段池，按需从中分配内存段加入 segments 列表。
     */
    protected final LinkedList<MemorySegment> freeSegments;

    /**
     * {@link BufferRecycler} used to recycle {@link #freeSegments}.
     *
     * <p>缓冲区回收器，release 时将 segments 中的内存段归还给内存池。
     */
    protected final BufferRecycler bufferRecycler;

    /**
     * A segment list as a joint buffer which stores all records and index entries.
     *
     * <p>联合缓冲区，所有子分区的索引项和数据都顺序存储在这些内存段中。
     * 作为一个逻辑连续的大缓冲区使用。
     */
    public final ArrayList<MemorySegment> segments = new ArrayList<>();

    /**
     * Addresses of the first record's index entry for each subpartition.
     *
     * <p>每个子分区第一条记录的索引项地址。读取时从这里开始遍历索引链表。
     * 初始值 -1 表示该子分区无数据。
     */
    private final long[] firstIndexEntryAddresses;

    /**
     * Addresses of the last record's index entry for each subpartition.
     *
     * <p>每个子分区最后一条记录的索引项地址。写入时用于链接新索引项到链表尾部。
     * 初始值 -1 表示该子分区无数据。
     */
    protected final long[] lastIndexEntryAddresses;

    /**
     * Size of buffers requested from buffer pool. All buffers must be of the same size.
     *
     * <p>统一的缓冲区大小（字节），用于计算地址偏移和容量。
     */
    private final int bufferSize;

    /**
     * Number of guaranteed buffers can be allocated from the buffer pool for data sort.
     *
     * <p>保证可分配的缓冲区数量上限，用于写入前的容量预检查。
     */
    private final int numGuaranteedBuffers;

    // =============================================================================================
    // 统计信息和状态标志
    // =============================================================================================

    /**
     * Total number of bytes already appended to this sort buffer.
     *
     * <p>已写入的总字节数（不含索引项开销），用于统计和判断缓冲区使用情况。
     */
    private long numTotalBytes;

    /**
     * Total number of records already appended to this sort buffer.
     *
     * <p>已写入的记录总数，用于监控和统计。
     */
    private long numTotalRecords;

    /**
     * Total number of bytes already read from this sort buffer.
     *
     * <p>已读取的字节总数，用于判断是否还有剩余数据（与 numTotalBytes 对比）。
     */
    protected long numTotalBytesRead;

    /**
     * Whether this sort buffer is finished. One can only read a finished sort buffer.
     *
     * <p>是否已完成写入。为 true 时表示进入只读状态，不能再追加数据。
     */
    protected boolean isFinished;

    /**
     * Whether this sort buffer is released. A released sort buffer can not be used.
     *
     * <p>是否已释放。为 true 时表示资源已回收，不能再进行任何操作。
     */
    protected boolean isReleased;

    // =============================================================================================
    // 写入相关字段
    // =============================================================================================

    /**
     * Array index in the segment list of the current available buffer for writing.
     *
     * <p>当前写入位置所在的内存段索引（segments 数组中的下标）。
     */
    private int writeSegmentIndex;

    /**
     * Next position in the current available buffer for writing.
     *
     * <p>当前内存段中的写入偏移量（段内字节位置）。
     */
    private int writeSegmentOffset;

    // =============================================================================================
    // 读取相关字段
    // =============================================================================================

    /**
     * Data of different subpartitions in this sort buffer will be read in this order.
     *
     * <p>子分区读取顺序数组，决定各子分区数据的输出顺序。
     * 默认按子分区索引顺序 [0, 1, 2, ...]，也可以通过构造函数自定义顺序。
     */
    protected final int[] subpartitionReadOrder;

    /**
     * Index entry address of the current record or event to be read.
     *
     * <p>当前待读取记录的索引项地址（64 位编码：高 32 位段索引 + 低 32 位段偏移）。
     */
    protected long readIndexEntryAddress;

    /**
     * Record bytes remaining after last copy, which must be read first in next copy.
     *
     * <p>上次拷贝后剩余的记录字节数。当单条记录跨越目标缓冲区边界时，
     * 记录剩余字节数以便下次继续拷贝。
     */
    protected int recordRemainingBytes;

    /**
     * Used to index the current available subpartition to read data from.
     *
     * <p>当前正在读取的子分区在读取顺序数组中的索引位置。初始值 -1 表示尚未开始读取。
     */
    protected int readOrderIndex = -1;

    protected SortBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        checkArgument(bufferSize > INDEX_ENTRY_SIZE, "Buffer size is too small.");
        checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSize = bufferSize;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        checkState(numGuaranteedBuffers <= freeSegments.size(), "Wrong number of free segments.");
        this.firstIndexEntryAddresses = new long[numSubpartitions];
        this.lastIndexEntryAddresses = new long[numSubpartitions];

        // initialized with -1 means the corresponding subpartition has no data
        Arrays.fill(firstIndexEntryAddresses, -1L);
        Arrays.fill(lastIndexEntryAddresses, -1L);

        this.subpartitionReadOrder = new int[numSubpartitions];
        if (customReadOrder != null) {
            checkArgument(customReadOrder.length == numSubpartitions, "Illegal data read order.");
            System.arraycopy(customReadOrder, 0, this.subpartitionReadOrder, 0, numSubpartitions);
        } else {
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                this.subpartitionReadOrder[subpartition] = subpartition;
            }
        }
    }

    /**
     * No partial record will be written to this {@link SortBasedDataBuffer}, which means that
     * either all data of target record will be written or nothing will be written.
     *
     * <p>追加数据到指定子分区。采用原子写入策略：要么整条记录全部写入成功，要么完全不写入。
     *
     * <p>写入流程：
     * <ol>
     *   <li>容量检查：allocateBuffersForRecord 预分配足够空间</li>
     *   <li>写入索引项：writeIndex 创建索引项并链接到子分区链表</li>
     *   <li>写入数据：writeRecord 将记录数据顺序写入联合缓冲区</li>
     * </ol>
     *
     * @return 如果空间不足无法写入则返回 true，调用者需要 finish 并开始新的 DataBuffer
     */
    @Override
    public boolean append(ByteBuffer source, int targetSubpartition, Buffer.DataType dataType)
            throws IOException {
        checkArgument(source.hasRemaining(), "Cannot append empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();

        // return true directly if it can not allocate enough buffers for the given record
        if (!allocateBuffersForRecord(totalBytes)) {
            return true;
        }

        // write the index entry and record or event data
        writeIndex(targetSubpartition, totalBytes, dataType);
        writeRecord(source);

        ++numTotalRecords;
        numTotalBytes += totalBytes;

        return false;
    }

    /**
     * 写入索引项并维护子分区索引链表。
     *
     * <p>索引项结构（16 字节）：
     * <ul>
     *   <li>前 8 字节：记录长度（高 32 位）+ 数据类型（低 32 位）</li>
     *   <li>后 8 字节：下一个索引项地址（由后续写入时回填）</li>
     * </ul>
     *
     * <p>链表维护：
     * <ul>
     *   <li>更新 lastIndexEntryAddresses[subpartitionIndex] 指向当前索引项</li>
     *   <li>如果不是第一条记录，回填前一个索引项的"下一个地址"字段</li>
     *   <li>如果是第一条记录，更新 firstIndexEntryAddresses[subpartitionIndex]</li>
     * </ul>
     */
    private void writeIndex(int subpartitionIndex, int numRecordBytes, Buffer.DataType dataType) {
        MemorySegment segment = segments.get(writeSegmentIndex);

        // record length takes the high 32 bits and data type takes the low 32 bits
        segment.putLong(writeSegmentOffset, ((long) numRecordBytes << 32) | dataType.ordinal());

        // segment index takes the high 32 bits and segment offset takes the low 32 bits
        long indexEntryAddress = ((long) writeSegmentIndex << 32) | writeSegmentOffset;

        long lastIndexEntryAddress = lastIndexEntryAddresses[subpartitionIndex];
        lastIndexEntryAddresses[subpartitionIndex] = indexEntryAddress;

        if (lastIndexEntryAddress >= 0) {
            // link the previous index entry of the given subpartition to the new index entry
            segment = segments.get(getSegmentIndexFromPointer(lastIndexEntryAddress));
            segment.putLong(
                    getSegmentOffsetFromPointer(lastIndexEntryAddress) + 8, indexEntryAddress);
        } else {
            firstIndexEntryAddresses[subpartitionIndex] = indexEntryAddress;
        }

        // move the write position forward so as to write the corresponding record
        updateWriteSegmentIndexAndOffset(INDEX_ENTRY_SIZE);
    }

    /**
     * 将记录数据写入联合缓冲区。
     *
     * <p>数据可能跨越多个内存段，循环写入直到所有数据写完。
     */
    private void writeRecord(ByteBuffer source) {
        while (source.hasRemaining()) {
            MemorySegment segment = segments.get(writeSegmentIndex);
            int toCopy = Math.min(bufferSize - writeSegmentOffset, source.remaining());
            segment.put(writeSegmentOffset, source, toCopy);

            // 更新写入位置，准备写入剩余字节或下一条记录
            updateWriteSegmentIndexAndOffset(toCopy);
        }
    }

    /**
     * 为待写入的记录预分配足够的缓冲区空间。
     *
     * <p>分配策略：
     * <ol>
     *   <li>计算所需空间：INDEX_ENTRY_SIZE + 记录长度</li>
     *   <li>如果当前可用空间不足以容纳索引项，跳过剩余空间（因为索引项不能跨段）</li>
     *   <li>检查总可用空间（当前 + 可分配）是否足够</li>
     *   <li>按需从 freeSegments 分配新的内存段</li>
     * </ol>
     *
     * @return 如果空间足够则返回 true，否则返回 false 表示 DataBuffer 已满
     */
    private boolean allocateBuffersForRecord(int numRecordBytes) {
        int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
        int availableBytes =
                writeSegmentIndex == segments.size() ? 0 : bufferSize - writeSegmentOffset;

        // return directly if current available bytes is adequate
        if (availableBytes >= numBytesRequired) {
            return true;
        }

        // skip the remaining free space if the available bytes is not enough for an index entry
        if (availableBytes < INDEX_ENTRY_SIZE) {
            updateWriteSegmentIndexAndOffset(availableBytes);
            availableBytes = 0;
        }

        if (availableBytes + (numGuaranteedBuffers - segments.size()) * (long) bufferSize
                < numBytesRequired) {
            return false;
        }

        // allocate exactly enough buffers for the appended record
        do {
            MemorySegment segment = freeSegments.poll();
            availableBytes += bufferSize;
            addBuffer(checkNotNull(segment));
        } while (availableBytes < numBytesRequired);

        return true;
    }

    private void addBuffer(MemorySegment segment) {
        if (segment.size() != bufferSize) {
            bufferRecycler.recycle(segment);
            throw new IllegalStateException("Illegal memory segment size.");
        }

        if (isReleased) {
            bufferRecycler.recycle(segment);
            throw new IllegalStateException("Sort buffer is already released.");
        }

        segments.add(segment);
    }

    private void updateWriteSegmentIndexAndOffset(int numBytes) {
        writeSegmentOffset += numBytes;

        // using the next available free buffer if the current is full
        if (writeSegmentOffset == bufferSize) {
            ++writeSegmentIndex;
            writeSegmentOffset = 0;
        }
    }

    /**
     * 将记录或事件数据从联合缓冲区拷贝到目标内存段。
     *
     * <p>核心读取方法，处理以下复杂情况：
     * <ul>
     *   <li><b>跨段读取</b>：源数据可能跨越多个 MemorySegment</li>
     *   <li><b>部分拷贝</b>：目标缓冲区可能装不下整条记录，需要记录剩余字节数</li>
     *   <li><b>断点续读</b>：如果上次有剩余（recordRemainingBytes > 0），从断点继续</li>
     * </ul>
     *
     * @param targetSegment 目标内存段
     * @param targetSegmentOffset 目标段的起始写入位置
     * @param sourceSegmentIndex 源数据起始段索引
     * @param sourceSegmentOffset 源数据起始段偏移
     * @param recordLength 完整记录长度
     * @return 本次实际拷贝的字节数
     */
    protected int copyRecordOrEvent(
            MemorySegment targetSegment,
            int targetSegmentOffset,
            int sourceSegmentIndex,
            int sourceSegmentOffset,
            int recordLength) {
        if (recordRemainingBytes > 0) {
            // skip the data already read if there is remaining partial record after the previous
            // copy
            long position = (long) sourceSegmentOffset + (recordLength - recordRemainingBytes);
            sourceSegmentIndex += (position / bufferSize);
            sourceSegmentOffset = (int) (position % bufferSize);
        } else {
            recordRemainingBytes = recordLength;
        }

        int targetSegmentSize = targetSegment.size();
        int numBytesToCopy =
                Math.min(targetSegmentSize - targetSegmentOffset, recordRemainingBytes);
        do {
            // move to next data buffer if all data of the current buffer has been copied
            if (sourceSegmentOffset == bufferSize) {
                ++sourceSegmentIndex;
                sourceSegmentOffset = 0;
            }

            int sourceRemainingBytes =
                    Math.min(bufferSize - sourceSegmentOffset, recordRemainingBytes);
            int numBytes = Math.min(targetSegmentSize - targetSegmentOffset, sourceRemainingBytes);
            MemorySegment sourceSegment = segments.get(sourceSegmentIndex);
            sourceSegment.copyTo(sourceSegmentOffset, targetSegment, targetSegmentOffset, numBytes);

            recordRemainingBytes -= numBytes;
            targetSegmentOffset += numBytes;
            sourceSegmentOffset += numBytes;
        } while ((recordRemainingBytes > 0 && targetSegmentOffset < targetSegmentSize));

        return numBytesToCopy;
    }

    /**
     * 切换到下一个有数据的子分区并更新读取地址。
     *
     * <p>按 subpartitionReadOrder 顺序查找下一个有数据的子分区，
     * 跳过没有数据的子分区（firstIndexEntryAddresses[i] == -1）。
     */
    protected void updateReadSubpartitionAndIndexEntryAddress() {
        // 跳过没有数据的子分区
        while (++readOrderIndex < firstIndexEntryAddresses.length) {
            int subpartitionIndex = subpartitionReadOrder[readOrderIndex];
            if ((readIndexEntryAddress = firstIndexEntryAddresses[subpartitionIndex]) >= 0) {
                break;
            }
        }
    }

    /**
     * 从 64 位地址指针中提取段索引（高 32 位）。
     *
     * <p>地址编码格式：[段索引(32位) | 段内偏移(32位)]
     */
    protected int getSegmentIndexFromPointer(long value) {
        return (int) (value >>> 32);
    }

    /**
     * 从 64 位地址指针中提取段内偏移（低 32 位）。
     *
     * <p>地址编码格式：[段索引(32位) | 段内偏移(32位)]
     */
    protected int getSegmentOffsetFromPointer(long value) {
        return (int) (value);
    }

    @Override
    public long numTotalRecords() {
        return numTotalRecords;
    }

    @Override
    public long numTotalBytes() {
        return numTotalBytes;
    }

    @Override
    public boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    /**
     * 完成写入阶段，进入可读取状态。
     *
     * <p>调用 updateReadSubpartitionAndIndexEntryAddress 初始化读取位置，
     * 定位到第一个有数据的子分区的第一条记录。
     */
    @Override
    public void finish() {
        checkState(!isFinished, "DataBuffer is already finished.");

        isFinished = true;

        // 初始化读取位置，准备开始读取
        updateReadSubpartitionAndIndexEntryAddress();
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    /**
     * 释放所有资源。
     *
     * <p>将所有内存段通过 bufferRecycler 归还给内存池，并清空 segments 列表。
     */
    @Override
    public void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;

        for (MemorySegment segment : segments) {
            bufferRecycler.recycle(segment);
        }
        segments.clear();
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }
}
