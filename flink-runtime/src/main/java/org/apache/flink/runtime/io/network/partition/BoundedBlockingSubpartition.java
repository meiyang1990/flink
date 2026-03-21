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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the ResultSubpartition for a bounded result transferred in a blocking
 * manner: The result is first produced, then consumed. The result can be consumed possibly multiple
 * times.
 *
 * <p>Depending on the supplied implementation of {@link BoundedData}, the actual data is stored for
 * example in a file, or in a temporary memory mapped file.
 *
 * <h2>有界阻塞子分区 - 中文说明</h2>
 *
 * <p>这是 ResultSubpartition 的批处理模式实现，用于需要先完整生产再消费的数据交换场景。
 *
 * <h3>核心特点</h3>
 * <ul>
 *   <li><b>阻塞式交换</b>：数据先完全写入，写入完成后才能开始读取</li>
 *   <li><b>支持多次消费</b>：同一份数据可以被多个消费者读取（如广播、多消费场景）</li>
 *   <li><b>可配置存储</b>：通过 {@link BoundedData} 抽象支持多种存储后端</li>
 *   <li><b>支持压缩</b>：写入时可选择压缩数据以减少磁盘占用</li>
 * </ul>
 *
 * <h3>存储后端选项</h3>
 * <ul>
 *   <li><b>FileChannel</b>：数据直接写入文件，读取时通过 FileRegion 零拷贝传输</li>
 *   <li><b>MemoryMapped</b>：使用内存映射文件，数据按需换入/换出</li>
 *   <li><b>FileChannel + MemoryMapped Reader</b>：写入用 FileChannel，读取用内存映射</li>
 * </ul>
 *
 * <h3>典型使用场景</h3>
 * <ul>
 *   <li>批处理作业的 Shuffle 数据交换</li>
 *   <li>Sort-Merge Shuffle 的输出分区</li>
 *   <li>需要多次读取同一数据的场景（如 Broadcast Join）</li>
 * </ul>
 *
 * <h2>Important Notes on Thread Safety - 线程安全说明</h2>
 *
 * <p>This class does not synchronize every buffer access. It assumes the threading model of the
 * Flink network stack and is not thread-safe beyond that.
 *
 * <p>本类不对每次缓冲区访问进行同步，它假设遵循 Flink 网络栈的线程模型：
 *
 * <ul>
 *   <li><b>单写入线程</b>：同一线程负责 add、flush、finish 和写入期间的 release</li>
 *   <li><b>多读取线程</b>：支持多个并发读取者，但每个 reader 只能由单线程使用</li>
 *   <li><b>reader 释放</b>：释放 reader 的线程必须与使用 reader 的线程相同</li>
 * </ul>
 *
 * <p><b>重要</b>：reader 释放后，不得再访问从该 reader 获取的 buffer，否则可能导致段错误！
 *
 * <p>This class assumes a single writer thread that adds buffers, flushes, and finishes the write
 * phase. That same thread is also assumed to perform the partition release, if the release happens
 * during the write phase.
 *
 * <p>The implementation supports multiple concurrent readers, but assumes a single thread per
 * reader. That same thread must also release the reader. In particular, after the reader was
 * released, no buffers obtained from this reader may be accessed any more, or segmentation faults
 * might occur in some implementations.
 *
 * <p>The method calls to create readers, dispose readers, and dispose the partition are thread-safe
 * vis-a-vis each other.
 */
final class BoundedBlockingSubpartition extends ResultSubpartition {

    /**
     * 同步锁，用于保护 reader 的创建和释放以及内存映射文件的销毁。
     * 这些操作需要线程安全以避免资源竞争。
     */
    private final Object lock = new Object();

    /**
     * 当前正在填充的缓冲区。
     * 可能随时间逐步填充数据，调用 flush 时写入 BoundedData。
     */
    @Nullable private BufferConsumer currentBuffer;

    /**
     * 有界数据存储。
     * 负责实际的数据持久化，可能是文件、内存映射文件等。
     */
    private final BoundedData data;

    /**
     * 所有已创建但尚未释放的 reader 集合。
     * 用于追踪活跃的消费者，在释放分区时需要等待所有 reader 关闭。
     */
    @GuardedBy("lock")
    private final Set<ResultSubpartitionView> readers;

    /**
     * 是否使用直接文件传输（FileRegion）。
     * 当分区类型是文件且未启用 SSL 时为 true，可以实现零拷贝网络传输。
     */
    private final boolean useDirectFileTransfer;

    /**
     * 已写入的数据缓冲区数量（不包括事件）。
     * 用于统计和消费者的 backlog 计算。
     */
    private int numDataBuffersWritten;

    /**
     * 已写入的缓冲区和事件总数。
     * 包括数据缓冲区和控制事件（如 EndOfPartitionEvent）。
     */
    private int numBuffersAndEventsWritten;

    /**
     * 写入完成标志。
     * 为 true 时表示数据已完全写入，可以创建 reader 进行读取。
     */
    private boolean isFinished;

    /**
     * 分区释放标志。
     * 为 true 时表示分区正在或已经被释放，不再接受新数据。
     */
    private boolean isReleased;

    public BoundedBlockingSubpartition(
            int index, ResultPartition parent, BoundedData data, boolean useDirectFileTransfer) {

        super(index, parent);

        this.data = checkNotNull(data);
        this.useDirectFileTransfer = useDirectFileTransfer;
        this.readers = new HashSet<>();
    }

    // ------------------------------------------------------------------------

    /**
     * Checks if writing is finished. Readers cannot be created until writing is finished, and no
     * further writes can happen after that.
     */
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    /**
     * 添加缓冲区到子分区。
     *
     * <p>如果分区已完成写入，直接关闭传入的缓冲区并返回错误码。
     * 否则先刷新当前缓冲区，然后设置新的缓冲区。
     *
     * @param bufferConsumer 要添加的缓冲区消费者
     * @param partialRecordLength 部分记录长度（此实现中未使用）
     * @return 可接受的最大缓冲区数量，或错误码
     */
    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) throws IOException {
        if (isFinished()) {
            bufferConsumer.close();
            return ADD_BUFFER_ERROR_CODE;
        }

        // 先刷新之前的缓冲区
        flushCurrentBuffer();
        currentBuffer = bufferConsumer;
        return Integer.MAX_VALUE;
    }

    /**
     * 刷新当前缓冲区。
     *
     * <p>注意：flush 方法签名不允许抛出异常，因此这里使用了运行时异常包装，
     * 这是一种不推荐但必要的模式。
     */
    @Override
    public void flush() {
        try {
            flushCurrentBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 将当前缓冲区写入持久化存储。
     */
    private void flushCurrentBuffer() throws IOException {
        if (currentBuffer != null) {
            writeAndCloseBufferConsumer(currentBuffer);
            currentBuffer = null;
        }
    }

    /**
     * 将缓冲区内容写入 BoundedData 存储。
     *
     * <p>处理流程：
     * <ol>
     *   <li>从 BufferConsumer 构建 Buffer</li>
     *   <li>如果可压缩，进行压缩处理</li>
     *   <li>写入 BoundedData 存储</li>
     *   <li>更新统计计数器</li>
     *   <li>释放资源</li>
     * </ol>
     */
    private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer) throws IOException {
        try {
            final Buffer buffer = bufferConsumer.build();
            try {
                // 检查是否可以压缩
                if (parent.canBeCompressed(buffer)) {
                    final Buffer compressedBuffer =
                            parent.bufferCompressor.compressToIntermediateBuffer(buffer);
                    data.writeBuffer(compressedBuffer);
                    // 如果压缩生成了新的 buffer，需要回收
                    if (compressedBuffer != buffer) {
                        compressedBuffer.recycleBuffer();
                    }
                } else {
                    data.writeBuffer(buffer);
                }

                // 更新统计
                numBuffersAndEventsWritten++;
                if (buffer.isBuffer()) {
                    numDataBuffersWritten++;
                }
            } finally {
                buffer.recycleBuffer();
            }
        } finally {
            bufferConsumer.close();
        }
    }

    /**
     * 完成子分区的写入。
     *
     * <p>处理流程：
     * <ol>
     *   <li>刷新剩余的当前缓冲区</li>
     *   <li>写入 EndOfPartitionEvent 标记数据结束</li>
     *   <li>通知 BoundedData 写入完成</li>
     * </ol>
     *
     * <p>调用后分区进入可读状态，消费者可以创建 reader 读取数据。
     *
     * @return EndOfPartitionEvent 的字节数
     */
    @Override
    public int finish() throws IOException {
        checkState(!isReleased, "data partition already released");
        checkState(!isFinished, "data partition already finished");

        isFinished = true;
        flushCurrentBuffer();
        // 写入分区结束事件
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        writeAndCloseBufferConsumer(eventBufferConsumer);
        // 通知存储层写入完成
        data.finishWrite();
        return eventBufferConsumer.getWrittenBytes();
    }

    /**
     * 释放子分区资源。
     *
     * <p>释放过程：
     * <ol>
     *   <li>设置释放和完成标志</li>
     *   <li>关闭当前未完成的缓冲区</li>
     *   <li>检查是否可以销毁底层存储（需等待所有 reader 释放）</li>
     * </ol>
     */
    @Override
    public void release() throws IOException {
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            isFinished = true; // 快速失败：阻止后续写入

            // 清理当前缓冲区
            if (currentBuffer != null) {
                currentBuffer.close();
                currentBuffer = null;
            }
            // 尝试释放底层存储
            checkReaderReferencesAndDispose();
        }
    }

    /**
     * 创建读取视图（reader）。
     *
     * <p>前提条件：
     * <ul>
     *   <li>分区未被释放</li>
     *   <li>写入已完成（isFinished = true）</li>
     *   <li>数据文件可读</li>
     * </ul>
     *
     * <p>根据配置返回不同的 reader 实现：
     * <ul>
     *   <li>DirectTransferReader：使用 FileRegion 零拷贝传输</li>
     *   <li>BoundedBlockingSubpartitionReader：通用 reader</li>
     * </ul>
     */
    @Override
    public ResultSubpartitionView createReadView(BufferAvailabilityListener availability)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "data partition already released");
            checkState(isFinished, "writing of blocking partition not yet finished");

            // 验证文件可读
            if (!Files.isReadable(data.getFilePath())) {
                throw new PartitionNotFoundException(parent.getPartitionId());
            }

            final ResultSubpartitionView reader;
            if (useDirectFileTransfer) {
                // 零拷贝传输：直接使用 FileRegion 发送文件内容
                reader =
                        new BoundedBlockingSubpartitionDirectTransferReader(
                                this,
                                data.getFilePath(),
                                numDataBuffersWritten,
                                numBuffersAndEventsWritten);
            } else {
                // 通用读取：通过 BoundedData 接口读取
                reader =
                        new BoundedBlockingSubpartitionReader(
                                this, data, numDataBuffersWritten, availability);
            }
            // 注册 reader 以追踪生命周期
            readers.add(reader);
            return reader;
        }
    }

    /**
     * 释放 reader 引用。
     *
     * <p>当 reader 关闭时调用，从追踪集合中移除并检查是否可以释放底层存储。
     */
    void releaseReaderReference(ResultSubpartitionView reader) throws IOException {
        onConsumedSubpartition();

        synchronized (lock) {
            if (readers.remove(reader) && isReleased) {
                // 分区已标记释放且所有 reader 都已关闭，可以销毁存储
                checkReaderReferencesAndDispose();
            }
        }
    }

    /**
     * 检查 reader 引用并尝试销毁底层存储。
     *
     * <p>为避免内存映射文件长时间占用资源，我们不等待 GC 来 unmap 文件，
     * 而是使用 Netty 工具直接 unmap。但为避免段错误，必须等待所有 reader 释放。
     */
    @GuardedBy("lock")
    private void checkReaderReferencesAndDispose() throws IOException {
        assert Thread.holdsLock(lock);

        // 只有当所有 reader 都已释放时才能安全销毁
        if (readers.isEmpty()) {
            data.close();
        }
    }

    @VisibleForTesting
    public BufferConsumer getCurrentBuffer() {
        return currentBuffer;
    }

    // ---------------------------- statistics - 统计信息 --------------------------------

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        // 有界阻塞分区不支持动态调整缓冲区大小
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return numBuffersAndEventsWritten;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return data.getSize();
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) {
        // 批处理分区不涉及 Checkpoint barrier 对齐
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // 批处理分区不涉及 Checkpoint
    }

    /** 获取 backlog 中的缓冲区数量（用于流控） */
    int getBuffersInBacklogUnsafe() {
        return numDataBuffersWritten;
    }

    // ---------------------------- factories - 工厂方法 --------------------------------

    /**
     * Creates a BoundedBlockingSubpartition that simply stores the partition data in a file. Data
     * is eagerly spilled (written to disk) and readers directly read from the file.
     *
     * <p>创建基于文件的有界阻塞分区。
     *
     * <p>特点：
     * <ul>
     *   <li>数据立即写入磁盘（eager spilling）</li>
     *   <li>读取时直接从文件读取</li>
     *   <li>未启用 SSL 时支持 FileRegion 零拷贝传输</li>
     * </ul>
     */
    public static BoundedBlockingSubpartition createWithFileChannel(
            int index,
            ResultPartition parent,
            File tempFile,
            int readBufferSize,
            boolean sslEnabled)
            throws IOException {

        final FileChannelBoundedData bd =
                FileChannelBoundedData.create(tempFile.toPath(), readBufferSize);
        // 只有未启用 SSL 时才能使用直接文件传输
        return new BoundedBlockingSubpartition(index, parent, bd, !sslEnabled);
    }

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in memory mapped file.
     * Data is written to and read from the mapped memory region. Disk spilling happens lazily, when
     * the OS swaps out the pages from the memory mapped file.
     *
     * <p>创建基于内存映射文件的有界阻塞分区。
     *
     * <p>特点：
     * <ul>
     *   <li>数据写入内存映射区域</li>
     *   <li>磁盘溢写由操作系统透明处理（页面换出时）</li>
     *   <li>读写都通过内存映射，适合数据可能被多次读取的场景</li>
     * </ul>
     */
    public static BoundedBlockingSubpartition createWithMemoryMappedFile(
            int index, ResultPartition parent, File tempFile) throws IOException {

        final MemoryMappedBoundedData bd = MemoryMappedBoundedData.create(tempFile.toPath());
        return new BoundedBlockingSubpartition(index, parent, bd, false);
    }

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in a file and memory
     * maps that file for reading. Data is eagerly spilled (written to disk) and then mapped into
     * memory. The main difference to the {@link #createWithMemoryMappedFile(int, ResultPartition,
     * File)} variant is that no I/O is necessary when pages from the memory mapped file are
     * evicted.
     *
     * <p>创建文件写入 + 内存映射读取的混合分区。
     *
     * <p>特点：
     * <ul>
     *   <li>写入使用 FileChannel（eager spilling）</li>
     *   <li>读取使用内存映射</li>
     *   <li>与纯内存映射相比，页面被换出时无需额外 I/O</li>
     * </ul>
     */
    public static BoundedBlockingSubpartition createWithFileAndMemoryMappedReader(
            int index, ResultPartition parent, File tempFile) throws IOException {

        final FileChannelMemoryMappedBoundedData bd =
                FileChannelMemoryMappedBoundedData.create(tempFile.toPath());
        return new BoundedBlockingSubpartition(index, parent, bd, false);
    }
}
