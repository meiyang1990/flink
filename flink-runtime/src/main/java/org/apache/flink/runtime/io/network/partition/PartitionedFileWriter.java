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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * File writer which can write buffers and generate {@link PartitionedFile}. Data is written region
 * by region. Before writing a new region, the method {@link PartitionedFileWriter#startNewRegion}
 * must be called. After writing all data, the method {@link PartitionedFileWriter#finish} must be
 * called to close all opened files and return the target {@link PartitionedFile}.
 *
 * <p>分区文件写入器，用于创建 Sort-Merge Shuffle 的持久化文件。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>Region-by-Region 写入</b>：数据按 Region 组织，每个 Region 包含所有子分区的数据</li>
 *   <li><b>索引缓存优化</b>：索引数据优先缓存在内存中，减少写入次数</li>
 *   <li><b>广播优化</b>：支持 Broadcast Region，同一份数据被所有子分区共享</li>
 *   <li><b>非线程安全</b>：设计为单线程使用，由调用方保证同步</li>
 * </ul>
 *
 * <h2>写入流程</h2>
 * <pre>
 * 1. 创建 Writer: new PartitionedFileWriter(...)
 * 2. 开始新 Region: startNewRegion(isBroadcast)
 * 3. 写入 Buffers: writeBuffers(buffers)
 *    - 可重复调用，但同一子分区的数据必须连续写入
 * 4. 重复步骤 2-3 直到所有数据写完
 * 5. 完成写入: finish() -> 返回 PartitionedFile
 * </pre>
 *
 * <h2>文件写入顺序</h2>
 * <pre>
 * Region 0:
 *   writeBuffers([SP0-buf1, SP0-buf2, SP1-buf1, SP2-buf1, ...])
 *   -> 索引记录每个子分区的 offset 和 size
 * Region 1:
 *   writeBuffers([SP0-buf1, SP1-buf1, SP1-buf2, ...])
 *   -> 索引记录每个子分区的 offset 和 size
 * ...
 * finish():
 *   -> 写入最后一个 Region 的索引
 *   -> 关闭文件通道
 *   -> 返回 PartitionedFile
 * </pre>
 *
 * <h2>与其他组件的关系</h2>
 * <ul>
 *   <li>{@link SortMergeResultPartition}：调用此 Writer 写入排序后的数据</li>
 *   <li>{@link PartitionedFile}：写入完成后返回的持久化文件表示</li>
 * </ul>
 */
@NotThreadSafe
public class PartitionedFileWriter implements AutoCloseable {

    /** 索引缓冲区的最小大小，确保能容纳至少 50 个索引条目 */
    private static final int MIN_INDEX_BUFFER_SIZE = 50 * PartitionedFile.INDEX_ENTRY_SIZE;

    /**
     * Number of subpartitions. When writing a buffer, target subpartition must be in this range.
     *
     * <p>子分区数量，决定索引文件中每个 Region 的索引条目数
     */
    private final int numSubpartitions;

    /** 数据文件通道，用于写入 Shuffle 数据 */
    private final FileChannel dataFileChannel;

    /** 索引文件通道，用于写入索引条目 */
    private final FileChannel indexFileChannel;

    /** 数据文件路径 */
    private final Path dataFilePath;

    /** 索引文件路径 */
    private final Path indexFilePath;

    /**
     * Offset in the data file for each subpartition in the current region.
     *
     * <p>当前 Region 中每个子分区数据在数据文件中的起始偏移量。
     * 数组下标对应子分区索引
     */
    private final long[] subpartitionOffsets;

    /**
     * Data size written in bytes for each subpartition in the current region.
     *
     * <p>当前 Region 中每个子分区已写入的字节数。
     * 每个新 Region 开始时重置为 0
     */
    private final long[] subpartitionBytes;

    /**
     * Maximum number of bytes can be used to buffer index entries.
     *
     * <p>索引缓冲区的最大字节数。当缓冲区满且未达到最大值时会自动扩容
     */
    private final int maxIndexBufferSize;

    /**
     * A piece of unmanaged memory for caching of region index entries.
     *
     * <p>索引缓冲区，用于缓存 Region 的索引条目：
     * <ul>
     *   <li>每个索引条目 16 字节（8 字节 offset + 8 字节 size）</li>
     *   <li>缓冲区满时，要么扩容（如果未达上限），要么刷写到磁盘</li>
     *   <li>如果所有索引都能缓存，finish 时会将其传递给 PartitionedFile 用于加速读取</li>
     * </ul>
     */
    private ByteBuffer indexBuffer;

    /**
     * Whether all index entries are cached in the index buffer or not.
     *
     * <p>标记所有索引是否都被缓存。如果 true，finish 时索引缓存会传递给 PartitionedFile
     */
    private boolean allIndexEntriesCached = true;

    /** 已写入数据文件的总字节数 */
    private long totalBytesWritten;

    /** 已写入的 Region 数量 */
    private int numRegions;

    /** 数据文件中 Buffer 的总数量 */
    private long numBuffers;

    /**
     * Current subpartition to write buffers to.
     *
     * <p>当前正在写入的子分区索引。用于检测是否违反了"同一子分区数据必须连续写入"的约束
     */
    private int currentSubpartition = -1;

    /**
     * Broadcast region is an optimization for the broadcast partition which writes the same data to
     * all subpartitions. For a broadcast region, data is only written once and the indexes of all
     * subpartitions point to the same offset in the data file.
     *
     * <p>广播 Region 标记。广播模式下：
     * <ul>
     *   <li>数据只写入一次</li>
     *   <li>所有子分区的索引都指向相同的数据偏移量</li>
     *   <li>显著减少磁盘空间和 I/O</li>
     * </ul>
     */
    private boolean isBroadcastRegion;

    /** 标记 Writer 是否已完成（调用了 finish 方法） */
    private boolean isFinished;

    /** 标记 Writer 是否已关闭 */
    private boolean isClosed;

    /**
     * 子分区的写入顺序数组。
     *
     * <p>支持自定义子分区写入顺序，用于优化读取时的顺序 I/O。
     * 例如 [4, 5, 0, 1, 2, 3] 表示先写子分区 4、5，再写 0、1、2、3
     */
    private final int[] writeOrder;

    /** 当前 Region 之前的累计写入字节数，用于计算空子分区的偏移量 */
    private long preRegionTotalBytesWritten;

    public PartitionedFileWriter(
            int numSubpartitions, int maxIndexBufferSize, String basePath, int[] writeOrder)
            throws IOException {
        this(numSubpartitions, MIN_INDEX_BUFFER_SIZE, maxIndexBufferSize, basePath, writeOrder);
    }

    @VisibleForTesting
    PartitionedFileWriter(
            int numSubpartitions,
            int minIndexBufferSize,
            int maxIndexBufferSize,
            String basePath,
            int[] writeOrder)
            throws IOException {
        checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");
        checkArgument(maxIndexBufferSize > 0, "Illegal maximum index cache size.");
        checkArgument(basePath != null, "Base path must not be null.");

        this.numSubpartitions = numSubpartitions;
        this.maxIndexBufferSize = alignMaxIndexBufferSize(maxIndexBufferSize);
        this.subpartitionOffsets = new long[numSubpartitions];
        this.subpartitionBytes = new long[numSubpartitions];
        this.dataFilePath = new File(basePath + PartitionedFile.DATA_FILE_SUFFIX).toPath();
        this.indexFilePath = new File(basePath + PartitionedFile.INDEX_FILE_SUFFIX).toPath();
        this.writeOrder = checkNotNull(writeOrder);

        this.indexBuffer = ByteBuffer.allocate(minIndexBufferSize);
        BufferReaderWriterUtil.configureByteBuffer(indexBuffer);

        this.dataFileChannel = openFileChannel(dataFilePath);
        try {
            this.indexFileChannel = openFileChannel(indexFilePath);
        } catch (Throwable throwable) {
            // ensure that the data file channel is closed if any exception occurs
            IOUtils.closeQuietly(dataFileChannel);
            IOUtils.deleteFileQuietly(dataFilePath);
            throw throwable;
        }
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private int alignMaxIndexBufferSize(int maxIndexBufferSize) {
        return maxIndexBufferSize
                / PartitionedFile.INDEX_ENTRY_SIZE
                * PartitionedFile.INDEX_ENTRY_SIZE;
    }

    /**
     * Persists the region index of the current data region and starts a new region to write.
     *
     * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
     * exception occurs.
     *
     * <p>持久化当前 Region 的索引并开始新的 Region。
     *
     * @param isBroadcastRegion Whether it's a broadcast region. See {@link #isBroadcastRegion}.
     */
    public void startNewRegion(boolean isBroadcastRegion) throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        // 写入前一个 Region 的索引
        writeRegionIndex();
        this.isBroadcastRegion = isBroadcastRegion;
    }

    /**
     * 写入单个索引条目到索引缓冲区。
     *
     * <p>如果缓冲区已满，会尝试扩容或刷写到磁盘
     */
    private void writeIndexEntry(long subpartitionOffset, long numBytes) throws IOException {
        if (!indexBuffer.hasRemaining()) {
            // 缓冲区满，尝试扩容
            if (!extendIndexBufferIfPossible()) {
                // 无法扩容，刷写到磁盘
                flushIndexBuffer();
                indexBuffer.clear();
                // 标记索引未完全缓存
                allIndexEntriesCached = false;
            }
        }

        indexBuffer.putLong(subpartitionOffset);
        indexBuffer.putLong(numBytes);
    }

    /**
     * 尝试扩展索引缓冲区大小。
     *
     * @return 如果成功扩容返回 true，已达到最大值返回 false
     */
    private boolean extendIndexBufferIfPossible() {
        if (indexBuffer.capacity() >= maxIndexBufferSize) {
            return false;
        }

        int newIndexBufferSize = Math.min(maxIndexBufferSize, 2 * indexBuffer.capacity());
        ByteBuffer newIndexBuffer = ByteBuffer.allocate(newIndexBufferSize);
        indexBuffer.flip();
        newIndexBuffer.put(indexBuffer);
        BufferReaderWriterUtil.configureByteBuffer(newIndexBuffer);
        indexBuffer = newIndexBuffer;

        return true;
    }

    /**
     * 写入当前 Region 的所有子分区索引。
     *
     * <p>只有当 Region 中有实际数据时才写入索引。写入完成后重置状态准备下一个 Region
     */
    private void writeRegionIndex() throws IOException {
        // 只有有数据时才写入索引
        if (Arrays.stream(subpartitionBytes).sum() > 0) {
            // 更新空子分区的偏移量，确保连续性
            updateEmptySubpartitionOffsets();
            // 为每个子分区写入索引条目
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                writeIndexEntry(subpartitionOffsets[subpartition], subpartitionBytes[subpartition]);
            }

            // 重置状态准备下一个 Region
            currentSubpartition = -1;
            ++numRegions;
            Arrays.fill(subpartitionBytes, 0);
            preRegionTotalBytesWritten = totalBytesWritten;
        }
    }

    /**
     * Updates the offsets of subpartitions, ensuring that they are contiguous.
     *
     * <p>This method is necessary because empty subpartitions do not trigger an update to their
     * offsets during the usual process. As such, we need to ensure here that every subpartition,
     * including empty ones, has its offset updated to maintain continuity. This process involves
     * adjusting each subpartition's offset based on the sum of previous subpartitions' bytes,
     * ensuring seamless data handling and storage alignment.
     */
    private void updateEmptySubpartitionOffsets() {
        for (int i = 0; i < writeOrder.length; i++) {
            int currentSubPartition = writeOrder[i];

            if (subpartitionBytes[currentSubPartition] == 0) {
                if (i == 0) {
                    // For the first subpartition, set its offset to the current pre-region total
                    // bytes written if it's empty.
                    subpartitionOffsets[currentSubPartition] = preRegionTotalBytesWritten;
                } else {
                    // For non-first subpartitions, update the offset of an empty subpartition to be
                    // contiguous with the previous subpartition.
                    int preSubPartition = writeOrder[i - 1];
                    subpartitionOffsets[currentSubPartition] =
                            subpartitionOffsets[preSubPartition]
                                    + subpartitionBytes[preSubPartition];
                }
            }
        }
    }

    private void flushIndexBuffer() throws IOException {
        indexBuffer.flip();
        if (indexBuffer.limit() > 0) {
            BufferReaderWriterUtil.writeBuffer(indexFileChannel, indexBuffer);
        }
    }

    /**
     * Writes a list of {@link Buffer}s to this {@link PartitionedFile}. It guarantees that after
     * the return of this method, the target buffers can be released. In a data region, all data of
     * the same subpartition must be written together.
     *
     * <p>Note: The caller is responsible for recycling the target buffers and releasing the failed
     * {@link PartitionedFile} if any exception occurs.
     *
     * <p>写入一批 Buffer 到分区文件。注意：同一子分区的数据必须在同一个 Region 内连续写入
     */
    public void writeBuffers(List<BufferWithSubpartition> bufferWithSubpartitions)
            throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        if (bufferWithSubpartitions.isEmpty()) {
            return;
        }

        numBuffers += bufferWithSubpartitions.size();
        long expectedBytes;
        // 每个 Buffer 需要 header + data，所以数组大小是 Buffer 数量的 2 倍
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithSubpartitions.size()];

        // 根据是否广播模式选择不同的处理逻辑
        if (isBroadcastRegion) {
            expectedBytes = collectBroadcastBuffers(bufferWithSubpartitions, bufferWithHeaders);
        } else {
            expectedBytes = collectUnicastBuffers(bufferWithSubpartitions, bufferWithHeaders);
        }

        totalBytesWritten += expectedBytes;
        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
    }

    /**
     * 处理单播模式的 Buffer 收集。
     *
     * <p>记录每个子分区的偏移量和大小，验证同一子分区数据的连续性约束
     */
    private long collectUnicastBuffers(
            List<BufferWithSubpartition> bufferWithSubpartitions, ByteBuffer[] bufferWithHeaders) {
        long expectedBytes = 0;
        long fileOffset = totalBytesWritten;
        for (int i = 0; i < bufferWithSubpartitions.size(); i++) {
            int subpartition = bufferWithSubpartitions.get(i).getSubpartitionIndex();
            // 检测子分区切换，记录新子分区的起始偏移量
            if (subpartition != currentSubpartition) {
                // 确保同一子分区数据连续：如果该子分区已有数据，说明违反了约束
                checkState(
                        subpartitionBytes[subpartition] == 0,
                        "Must write data of the same subpartition together.");
                subpartitionOffsets[subpartition] = fileOffset;
                currentSubpartition = subpartition;
            }

            Buffer buffer = bufferWithSubpartitions.get(i).getBuffer();
            int numBytes = setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            expectedBytes += numBytes;
            fileOffset += numBytes;
            subpartitionBytes[subpartition] += numBytes;
        }
        return expectedBytes;
    }

    /**
     * 处理广播模式的 Buffer 收集。
     *
     * <p>广播模式下，所有子分区共享相同的数据，只需要写入一次但索引指向相同位置
     */
    private long collectBroadcastBuffers(
            List<BufferWithSubpartition> bufferWithSubpartitions, ByteBuffer[] bufferWithHeaders) {
        // 首次写入广播数据时，设置所有子分区的起始偏移量为当前文件位置
        if (subpartitionBytes[0] == 0) {
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                subpartitionOffsets[subpartition] = totalBytesWritten;
            }
        }

        long expectedBytes = 0;
        for (int i = 0; i < bufferWithSubpartitions.size(); i++) {
            Buffer buffer = bufferWithSubpartitions.get(i).getBuffer();
            int numBytes = setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            expectedBytes += numBytes;
        }

        // 广播模式：所有子分区记录相同的数据大小
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            subpartitionBytes[subpartition] += expectedBytes;
        }
        return expectedBytes;
    }

    /**
     * 为 Buffer 设置头部信息并放入数组。
     *
     * @return 头部 + 数据的总字节数
     */
    private int setBufferWithHeader(Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();

        return header.remaining() + buffer.readableBytes();
    }

    /**
     * Finishes writing the {@link PartitionedFile} which closes the file channel and returns the
     * corresponding {@link PartitionedFile}.
     *
     * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
     * exception occurs.
     *
     * <p>完成文件写入并返回 PartitionedFile 对象：
     * <ol>
     *   <li>写入最后一个 Region 的索引</li>
     *   <li>刷写索引缓冲区</li>
     *   <li>关闭文件通道</li>
     *   <li>如果所有索引都被缓存，将缓存传递给 PartitionedFile 用于加速读取</li>
     * </ol>
     */
    public PartitionedFile finish() throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        isFinished = true;

        // 写入最后一个 Region 的索引
        writeRegionIndex();
        flushIndexBuffer();
        indexBuffer.rewind();

        long dataFileSize = dataFileChannel.size();
        long indexFileSize = indexFileChannel.size();
        close();

        // 如果所有索引都被缓存，传递给 PartitionedFile 用于加速读取
        ByteBuffer indexEntryCache = null;
        if (allIndexEntriesCached) {
            indexEntryCache = indexBuffer;
        }
        indexBuffer = null;
        return new PartitionedFile(
                numRegions,
                numSubpartitions,
                dataFilePath,
                indexFilePath,
                dataFileSize,
                indexFileSize,
                numBuffers,
                indexEntryCache);
    }

    /** 静默释放资源：关闭 Writer 并删除已写入的文件，用于异常恢复 */
    public void releaseQuietly() {
        IOUtils.closeQuietly(this);
        IOUtils.deleteFileQuietly(dataFilePath);
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;

        IOException exception = null;
        try {
            dataFileChannel.close();
        } catch (IOException ioException) {
            exception = ioException;
        }

        try {
            indexFileChannel.close();
        } catch (IOException ioException) {
            exception = ExceptionUtils.firstOrSuppressed(ioException, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }
}
