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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Reader which can read all data of the target subpartition from a {@link PartitionedFile}.
 *
 * <p>分区文件读取器，用于从 Sort-Merge Shuffle 的持久化文件中读取指定子分区的数据。
 *
 * <h2>核心设计特点</h2>
 * <ul>
 *   <li><b>按 Region 顺序读取</b>：遍历所有 Region，从每个 Region 中读取目标子分区的数据</li>
 *   <li><b>支持多子分区读取</b>：可以一次读取多个连续子分区的数据（通过 ResultSubpartitionIndexSet）</li>
 *   <li><b>文件偏移优先级</b>：支持按文件偏移量排序，实现顺序 I/O 优化</li>
 *   <li><b>广播数据支持</b>：通过 repeatCount 处理广播数据的重复消费</li>
 * </ul>
 *
 * <h2>读取流程</h2>
 * <pre>
 * 1. 创建 Reader 并初始化 Region 索引
 * 2. 循环读取:
 *    a. moveToNextReadablePosition() - 移动到下一个可读位置
 *    b. readCurrentRegion() - 读取当前 Region 的数据
 *    c. 处理读取的 Buffer（通过 consumer 回调）
 * 3. 直到 hasRemaining() 返回 false
 * </pre>
 *
 * <h2>子分区旋转（Rotation）</h2>
 * <p>当子分区写入顺序被旋转时（例如 [4,5,0,1,2,3]），读取时需要正确处理这种旋转：
 * <ul>
 *   <li>subpartitionOrderRotationIndex 记录旋转点位置</li>
 *   <li>读取连续子分区时可能需要分两段读取（跨越旋转点）</li>
 * </ul>
 *
 * <h2>与其他组件的关系</h2>
 * <ul>
 *   <li>{@link PartitionedFile}：被读取的分区文件</li>
 *   <li>{@link SortMergeSubpartitionReader}：使用此 Reader 读取数据</li>
 *   <li>{@link SortMergeResultPartitionReadScheduler}：调度多个 Reader 实现顺序 I/O</li>
 * </ul>
 */
class PartitionedFileReader {

    /** 用于从文件中读取 Buffer 头部信息（数据类型、大小等） */
    private final ByteBuffer headerBuf;

    /** 用于从索引文件读取索引条目 */
    private final ByteBuffer indexEntryBuf;

    /** 目标分区文件 */
    private final PartitionedFile partitionedFile;

    /** 要读取的目标子分区集合（支持连续子分区范围） */
    private final ResultSubpartitionIndexSet subpartitionIndexSet;

    /** 数据文件通道（共享，由 ReadScheduler 管理生命周期） */
    private final FileChannel dataFileChannel;

    /** 索引文件通道（共享，由 ReadScheduler 管理生命周期） */
    private final FileChannel indexFileChannel;

    /**
     * Records the shift position of the subpartition write order. For example, if the write order
     * of subpartitions is [4, 5, 0, 1, 2, 3], then this value would be 4.
     *
     * <p>子分区写入顺序的旋转索引。用于处理子分区写入顺序优化场景：
     * 例如写入顺序为 [4,5,0,1,2,3]，则 rotationIndex=4，
     * 表示从物理顺序的第 4 个位置开始是逻辑子分区 0
     */
    private final int subpartitionOrderRotationIndex;

    /** 下一个要读取的 Region 索引 */
    private int nextRegionToRead;

    /** 下一个要读取的文件偏移量 */
    private long nextOffsetToRead;

    /** 当前 Region 剩余未读取的字节数 */
    private long currentRegionRemainingBytes;

    /** 待读取的 Buffer 位置描述队列，存储已解析的读取位置信息 */
    private final Queue<BufferPositionDescriptor> readBufferPositions = new ArrayDeque<>();

    /** 当前正在处理的 Buffer 位置描述符 */
    private BufferPositionDescriptor currentBufferPositionDescriptor;

    PartitionedFileReader(
            PartitionedFile partitionedFile,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel,
            ByteBuffer headerBuffer,
            ByteBuffer indexEntryBuffer,
            int subpartitionOrderRotationIndex) {
        checkArgument(checkNotNull(dataFileChannel).isOpen(), "Data file channel must be opened.");
        checkArgument(
                checkNotNull(indexFileChannel).isOpen(), "Index file channel must be opened.");

        this.partitionedFile = checkNotNull(partitionedFile);
        this.subpartitionIndexSet = subpartitionIndexSet;
        this.dataFileChannel = dataFileChannel;
        this.indexFileChannel = indexFileChannel;
        this.headerBuf = headerBuffer;
        this.indexEntryBuf = indexEntryBuffer;
        this.subpartitionOrderRotationIndex = subpartitionOrderRotationIndex;
    }

    /**
     * 移动到下一个可读位置。
     *
     * <p>该方法处理 Region 间和 Region 内的位置切换：
     * <ol>
     *   <li>如果当前 Region 数据已读完，检查 readBufferPositions 队列是否有待读数据</li>
     *   <li>如果队列为空，则移动到下一个 Region 并解析其索引</li>
     *   <li>更新 nextOffsetToRead 和 currentRegionRemainingBytes</li>
     * </ol>
     */
    private void moveToNextReadablePosition(ByteBuffer indexEntryBuf) throws IOException {
        while (currentRegionRemainingBytes <= 0 && hasNextPositionToRead()) {
            if (!readBufferPositions.isEmpty()) {
                // 从队列中取出下一个待读位置
                BufferPositionDescriptor descriptor = readBufferPositions.poll();
                nextOffsetToRead = descriptor.offset;
                currentRegionRemainingBytes = descriptor.size;
                currentBufferPositionDescriptor = descriptor;
            } else {
                // 队列为空，移动到下一个 Region
                if (nextRegionToRead < partitionedFile.getNumRegions()) {
                    // 解析下一个 Region 的索引，填充 readBufferPositions 队列
                    updateReadableOffsetAndSize(indexEntryBuf, readBufferPositions);
                    ++nextRegionToRead;
                }
            }
        }
    }

    /** 检查是否还有待读位置（队列非空或还有未处理的 Region） */
    private boolean hasNextPositionToRead() {
        return !readBufferPositions.isEmpty() || nextRegionToRead < partitionedFile.getNumRegions();
    }

    /**
     * Updates the readable offsets and sizes for subpartitions based on a given index buffer. This
     * method handles cases where the subpartition range is split by a rotation index, ensuring that
     * all necessary index entries are processed.
     *
     * <p>The method operates in the following way:
     *
     * <ol>
     *   <li>It checks if the range of subpartition indices requires handling of a wrap around the
     *       rotation index.
     *   <li>If no wrap is necessary (when the range does not cross the rotation point), it directly
     *       updates readable offsets and sizes for the entire range.
     *   <li>If a wrap is necessary, it splits the process into two updates:
     *       <ul>
     *         <li>Firstly, it updates from the rotation index to the end subpartition.
     *         <li>Secondly, it updates from the start subpartition to just before the rotation
     *             index.
     *       </ul>
     * </ol>
     *
     * <p>This ensures that all relevant subpartitions are correctly processed and offsets and sizes
     * are added to the queue for subsequent reading.
     *
     * @param indexEntryBuf A ByteBuffer containing index entries which provide offset and size
     *     information.
     * @param readBufferPositions A queue to store the buffer position descriptors.
     * @throws IOException If an I/O error occurs when accessing the index file channel.
     */
    @VisibleForTesting
    void updateReadableOffsetAndSize(
            ByteBuffer indexEntryBuf, Queue<BufferPositionDescriptor> readBufferPositions)
            throws IOException {
        int startSubpartition = subpartitionIndexSet.getStartIndex();
        int endSubpartition = subpartitionIndexSet.getEndIndex();

        if (startSubpartition >= subpartitionOrderRotationIndex
                || endSubpartition < subpartitionOrderRotationIndex) {
            updateReadableOffsetAndSize(
                    startSubpartition, endSubpartition, indexEntryBuf, readBufferPositions);
        } else {
            updateReadableOffsetAndSize(
                    subpartitionOrderRotationIndex,
                    endSubpartition,
                    indexEntryBuf,
                    readBufferPositions);
            updateReadableOffsetAndSize(
                    startSubpartition,
                    subpartitionOrderRotationIndex - 1,
                    indexEntryBuf,
                    readBufferPositions);
        }
    }

    /**
     * Updates the readable offsets and sizes for a specified range of subpartitions. If offsets are
     * contiguous, they are merged into a single entry. If not contiguous, each subpartition's
     * offset and size must come from the same buffer, and individual tuples are added for each
     * entry.
     *
     * @param startSubpartition The starting index of the subpartition range to be processed.
     * @param endSubpartition The ending index of the subpartition range to be processed.
     * @param indexEntryBuf A ByteBuffer containing the index entries to read offsets and sizes.
     * @param readBufferPositions A queue to store the buffer position descriptors.
     * @throws IOException If an I/O error occurs during reading of index entries.
     * @throws IllegalStateException If offsets are not contiguous and not from a single buffer.
     */
    private void updateReadableOffsetAndSize(
            int startSubpartition,
            int endSubpartition,
            ByteBuffer indexEntryBuf,
            Queue<BufferPositionDescriptor> readBufferPositions)
            throws IOException {
        partitionedFile.getIndexEntry(
                indexFileChannel, indexEntryBuf, nextRegionToRead, startSubpartition);
        long startPartitionOffset = indexEntryBuf.getLong();
        long startPartitionSize = indexEntryBuf.getLong();

        partitionedFile.getIndexEntry(
                indexFileChannel, indexEntryBuf, nextRegionToRead, endSubpartition);
        long endPartitionOffset = indexEntryBuf.getLong();
        long endPartitionSize = indexEntryBuf.getLong();

        if (startPartitionOffset != endPartitionOffset || startPartitionSize != endPartitionSize) {
            readBufferPositions.add(
                    new BufferPositionDescriptor(
                            startPartitionOffset,
                            endPartitionOffset + endPartitionSize - startPartitionOffset,
                            1));
        } else if (startPartitionSize != 0) {
            // this branch is for broadcast subpartitions
            readBufferPositions.add(
                    new BufferPositionDescriptor(
                            startPartitionOffset,
                            startPartitionSize,
                            endSubpartition - startSubpartition + 1));
        }
    }

    @VisibleForTesting
    void readCurrentRegion(
            Queue<MemorySegment> freeSegments, BufferRecycler recycler, Consumer<Buffer> consumer)
            throws IOException {
        readCurrentRegion(freeSegments, recycler, (buffer, repeatCount) -> consumer.accept(buffer));
    }

    /**
     * Reads a buffer from the current region of the target {@link PartitionedFile} and moves the
     * read position forward.
     *
     * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
     *
     * <p>从当前 Region 读取数据到提供的内存段中。核心读取逻辑：
     * <ol>
     *   <li>定位文件通道到目标偏移量</li>
     *   <li>循环读取数据直到内存段用尽或 Region 数据读完</li>
     *   <li>解析 Buffer 头部和数据，通过 consumer 回调处理</li>
     *   <li>处理跨 Buffer 边界的情况（部分读取的头部和数据）</li>
     * </ol>
     *
     * @param freeSegments The free {@link MemorySegment}s to read data to.
     * @param recycler The {@link BufferRecycler} which is responsible to recycle the target buffer.
     * @param consumer The target {@link Buffer} stores the data read from file channel.
     * @return Whether the file reader has remaining data to read.
     */
    boolean readCurrentRegion(
            Queue<MemorySegment> freeSegments,
            BufferRecycler recycler,
            BiConsumer<Buffer, Integer> consumer)
            throws IOException {
        if (currentRegionRemainingBytes == 0) {
            return false;
        }

        checkArgument(!freeSegments.isEmpty(), "No buffer available for data reading.");
        // 定位到目标偏移量
        dataFileChannel.position(nextOffsetToRead);

        // 用于跟踪跨 Buffer 边界的部分数据
        BufferAndHeader partialBuffer = new BufferAndHeader(null, null);
        try {
            // 循环读取直到内存段用尽或数据读完
            while (!freeSegments.isEmpty() && currentRegionRemainingBytes > 0) {
                MemorySegment segment = freeSegments.poll();
                int numBytes = (int) Math.min(segment.size(), currentRegionRemainingBytes);
                ByteBuffer byteBuffer = segment.wrap(0, numBytes);

                try {
                    // 从文件读取数据到内存段
                    BufferReaderWriterUtil.readByteBufferFully(dataFileChannel, byteBuffer);
                    byteBuffer.flip();
                    currentRegionRemainingBytes -= byteBuffer.remaining();
                    nextOffsetToRead += byteBuffer.remaining();
                } catch (Throwable throwable) {
                    // 读取失败，归还内存段
                    freeSegments.add(segment);
                    throw throwable;
                }

                // 包装为 NetworkBuffer 用于后续处理
                NetworkBuffer buffer = new NetworkBuffer(segment, recycler);
                buffer.setSize(byteBuffer.remaining());
                try {
                    // 解析 Buffer 头部和数据，处理跨边界情况
                    partialBuffer = processBuffer(byteBuffer, buffer, partialBuffer, consumer);
                } catch (Throwable throwable) {
                    partialBuffer = new BufferAndHeader(null, null);
                    throw throwable;
                } finally {
                    buffer.recycleBuffer();
                }
            }
        } finally {
            // 回滚部分读取的头部数据（下次读取时重新处理）
            if (headerBuf.position() > 0) {
                nextOffsetToRead -= headerBuf.position();
                currentRegionRemainingBytes += headerBuf.position();
                headerBuf.clear();
            }
            // 回滚部分读取的头部
            if (partialBuffer.header != null) {
                nextOffsetToRead -= HEADER_LENGTH;
                currentRegionRemainingBytes += HEADER_LENGTH;
            }
            // 回滚部分读取的数据
            if (partialBuffer.buffer != null) {
                nextOffsetToRead -= partialBuffer.buffer.readableBytes();
                currentRegionRemainingBytes += partialBuffer.buffer.readableBytes();
                partialBuffer.buffer.recycleBuffer();
            }
        }
        return hasRemaining();
    }

    /** 检查是否还有剩余数据可读 */
    boolean hasRemaining() throws IOException {
        moveToNextReadablePosition(indexEntryBuf);
        return currentRegionRemainingBytes > 0;
    }

    void initRegionIndex(ByteBuffer initIndexEntryBuffer) throws IOException {
        moveToNextReadablePosition(initIndexEntryBuffer);
    }

    /** 获取此 Reader 的读取优先级，值越小优先级越高（基于文件偏移量，实现顺序 I/O） */
    long getPriority() {
        return nextOffsetToRead;
    }

    /**
     * 处理读取到的数据，解析 Buffer 头部和内容。
     *
     * <p>该方法处理以下场景：
     * <ul>
     *   <li>完整 Buffer：头部和数据都在当前读取范围内</li>
     *   <li>部分头部：头部跨越两次读取</li>
     *   <li>部分数据：数据跨越两次读取（使用 CompositeBuffer 组合）</li>
     * </ul>
     *
     * @param byteBuffer 当前读取的原始数据
     * @param buffer 包装后的 NetworkBuffer
     * @param partialBuffer 上次遗留的部分数据
     * @param consumer 完整 Buffer 的消费回调
     * @return 本次遗留的部分数据（如果有）
     */
    private BufferAndHeader processBuffer(
            ByteBuffer byteBuffer,
            Buffer buffer,
            BufferAndHeader partialBuffer,
            BiConsumer<Buffer, Integer> consumer) {
        BufferHeader header = partialBuffer.header;
        CompositeBuffer targetBuffer = partialBuffer.buffer;
        while (byteBuffer.hasRemaining()) {
            if (header == null && (header = parseBufferHeader(byteBuffer)) == null) {
                break;
            }

            if (targetBuffer != null) {
                buffer.retainBuffer();
                int position = byteBuffer.position() + targetBuffer.missingLength();
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), targetBuffer.missingLength()));
                byteBuffer.position(position);
            } else if (byteBuffer.remaining() < header.getLength()) {
                if (byteBuffer.hasRemaining()) {
                    buffer.retainBuffer();
                    targetBuffer = new CompositeBuffer(header);
                    targetBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), byteBuffer.remaining()));
                }
                break;
            } else {
                buffer.retainBuffer();
                targetBuffer = new CompositeBuffer(header);
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
            }

            header = null;
            consumer.accept(targetBuffer, currentBufferPositionDescriptor.repeatCount);
            targetBuffer = null;
        }
        return new BufferAndHeader(targetBuffer, header);
    }

    /**
     * 解析 Buffer 头部信息。
     *
     * <p>处理头部跨读取边界的情况：
     * <ul>
     *   <li>如果 headerBuf 中有部分头部数据，继续填充直到完整</li>
     *   <li>如果当前数据不足以构成完整头部，暂存到 headerBuf</li>
     * </ul>
     */
    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        BufferHeader header = null;
        // 检查是否有上次遗留的部分头部
        if (headerBuf.position() > 0) {
            // 继续填充头部直到完整
            while (headerBuf.hasRemaining()) {
                headerBuf.put(buffer.get());
            }
            headerBuf.flip();
            header = BufferReaderWriterUtil.parseBufferHeader(headerBuf);
            headerBuf.clear();
        }

        // 检查当前数据是否足够解析头部
        if (header == null && buffer.remaining() < HEADER_LENGTH) {
            // 数据不足，暂存到 headerBuf
            headerBuf.put(buffer);
        } else if (header == null) {
            // 数据充足，直接解析
            header = BufferReaderWriterUtil.parseBufferHeader(buffer);
        }
        return header;
    }

    /**
     * 内部类：封装部分读取的 Buffer 和头部信息。
     *
     * <p>用于处理跨读取边界的情况，在下次读取时继续处理
     */
    private static class BufferAndHeader {

        /** 部分读取的 CompositeBuffer（可能跨多个内存段） */
        private final CompositeBuffer buffer;
        /** 已解析的头部信息 */
        private final BufferHeader header;

        BufferAndHeader(CompositeBuffer buffer, BufferHeader header) {
            this.buffer = buffer;
            this.header = header;
        }
    }

    /**
     * Represents the position and size of a buffer along with the repeat count. For a regular
     * buffer, the repeat count is typically one. For a broadcast buffer, the repeat count
     * corresponds to the number of subpartitions.
     *
     * <p>Buffer 位置描述符，记录待读数据在文件中的位置信息。
     *
     * <h3>字段说明</h3>
     * <ul>
     *   <li>offset：数据在文件中的起始偏移量</li>
     *   <li>size：数据大小（字节）</li>
     *   <li>repeatCount：重复计数，用于广播数据（广播时多个子分区共享同一份数据，
     *       repeatCount 等于子分区数量，便于下游正确消费）</li>
     * </ul>
     */
    @VisibleForTesting
    static class BufferPositionDescriptor {
        private final long offset;
        private final long size;
        private final int repeatCount;

        /**
         * Constructs a BufferPositionDescriptor with specified offset, size, and repeat count.
         *
         * @param offset the offset of the buffer
         * @param size the size of the buffer
         * @param repeatCount the repeat count for the buffer
         */
        BufferPositionDescriptor(long offset, long size, int repeatCount) {
            this.offset = offset;
            this.size = size;
            this.repeatCount = repeatCount;
        }

        @VisibleForTesting
        long getOffset() {
            return offset;
        }

        @VisibleForTesting
        long getSize() {
            return size;
        }

        @VisibleForTesting
        int getRepeatCount() {
            return repeatCount;
        }
    }
}
