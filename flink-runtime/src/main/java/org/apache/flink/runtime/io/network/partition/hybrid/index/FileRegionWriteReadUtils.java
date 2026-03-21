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

import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex.FixedSizeRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Utils for read and write {@link FileDataIndexRegionHelper.Region}.
 *
 * <h2>核心功能</h2>
 *
 * <p>提供索引区域（Region）的文件读写工具方法，支持固定大小 Region 的序列化和反序列化。
 *
 * <h2>FixedSizeRegion 格式</h2>
 *
 * <p>固定大小 Region 的二进制格式（共 24 字节）：
 * <pre>
 * ┌─────────────────────────────────────────────────────┐
 * │ firstBufferIndex (4 bytes, int)                     │
 * ├─────────────────────────────────────────────────────┤
 * │ numBuffers (4 bytes, int)                           │
 * ├─────────────────────────────────────────────────────┤
 * │ firstBufferOffset (8 bytes, long)                   │
 * ├─────────────────────────────────────────────────────┤
 * │ lastBufferEndOffset (8 bytes, long)                 │
 * └─────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>字节序</h2>
 *
 * <p>使用本地字节序（native order），与 JVM 运行平台一致，避免不必要的字节转换开销。
 */
public class FileRegionWriteReadUtils {

    /**
     * 分配指定大小的直接字节缓冲区并配置为本地字节序。
     *
     * <p>使用直接缓冲区（DirectByteBuffer）可以避免 JVM 堆和本地内存之间的数据拷贝，
     * 提高文件 I/O 性能。
     *
     * @param bufferSize the size of buffer to allocate.
     * @return a native order buffer with expected size.
     */
    public static ByteBuffer allocateAndConfigureBuffer(int bufferSize) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        buffer.order(ByteOrder.nativeOrder());
        return buffer;
    }

    /**
     * 将固定大小的 Region 写入文件通道。
     *
     * <p>写入流程：
     * <ol>
     *   <li>清空并重置 regionBuffer
     *   <li>按顺序写入 4 个字段：firstBufferIndex, numBuffers, regionStartOffset, regionEndOffset
     *   <li>翻转缓冲区并写入文件通道
     * </ol>
     *
     * <p><b>注意</b>：此类型的 Region 长度固定，不包含可变长度数据。
     *
     * @param channel the file's channel to write.
     * @param regionBuffer the buffer to write {@link FixedSizeRegion}'s header.
     * @param region the region to be written to channel.
     */
    public static void writeFixedSizeRegionToFile(
            FileChannel channel, ByteBuffer regionBuffer, FileDataIndexRegionHelper.Region region)
            throws IOException {
        regionBuffer.clear();
        regionBuffer.putInt(region.getFirstBufferIndex());
        regionBuffer.putInt(region.getNumBuffers());
        regionBuffer.putLong(region.getRegionStartOffset());
        regionBuffer.putLong(region.getRegionEndOffset());
        regionBuffer.flip();
        BufferReaderWriterUtil.writeBuffers(channel, regionBuffer.capacity(), regionBuffer);
    }

    /**
     * 从文件通道读取固定大小的 Region。
     *
     * <p>读取流程：
     * <ol>
     *   <li>清空并重置 regionBuffer
     *   <li>从指定文件偏移位置读取完整的 Region 数据
     *   <li>翻转缓冲区，按顺序解析 4 个字段
     *   <li>构造并返回 FixedSizeRegion 对象
     * </ol>
     *
     * <p><b>注意</b>：此类型的 Region 长度固定，读取时无需先获取长度信息。
     *
     * @param channel the channel to read.
     * @param regionBuffer the buffer to read {@link FixedSizeRegion}'s header.
     * @param fileOffset the file offset to start read.
     * @return the {@link FixedSizeRegion} that read from this channel.
     */
    public static FixedSizeRegion readFixedSizeRegionFromFile(
            FileChannel channel, ByteBuffer regionBuffer, long fileOffset) throws IOException {
        regionBuffer.clear();
        BufferReaderWriterUtil.readByteBufferFully(channel, regionBuffer, fileOffset);
        regionBuffer.flip();
        int firstBufferIndex = regionBuffer.getInt();
        int numBuffers = regionBuffer.getInt();
        long firstBufferOffset = regionBuffer.getLong();
        long lastBufferEndOffset = regionBuffer.getLong();
        return new FixedSizeRegion(
                firstBufferIndex, firstBufferOffset, lastBufferEndOffset, numBuffers);
    }
}
