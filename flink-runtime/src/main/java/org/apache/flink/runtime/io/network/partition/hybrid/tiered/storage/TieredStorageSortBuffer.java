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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferWithSubpartition;
import org.apache.flink.runtime.io.network.partition.SortBasedDataBuffer;
import org.apache.flink.runtime.io.network.partition.SortBuffer;

import javax.annotation.Nullable;

import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * When getting buffers, The {@link SortBasedDataBuffer} need not recycle the read target buffer..
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageSortBuffer 继承自 {@link SortBuffer}，专门为分层存储（Tiered Storage）
 * 定制的排序缓冲区实现。与父类的主要区别在于：
 *
 * <ul>
 *   <li><b>缓冲区回收策略</b>: 读取时不回收 transitBuffer，由调用方负责管理</li>
 *   <li><b>部分记录支持</b>: 可配置是否允许记录跨缓冲区拆分</li>
 *   <li><b>记录边界标记</b>: 当不允许部分记录时，标记 DATA_BUFFER_WITH_CLEAR_END</li>
 * </ul>
 *
 * <h2>读取流程</h2>
 *
 * <pre>
 *   getNextBuffer(transitBuffer)
 *           │
 *           ▼
 *   ┌───────────────────────┐
 *   │ 按子分区顺序读取索引项 │
 *   └───────────────────────┘
 *           │
 *           ▼
 *   ┌───────────────────────┐
 *   │ 解析 length+dataType │
 *   └───────────────────────┘
 *           │
 *      ┌────┴────┐
 *      │ 是事件？ │
 *      └────┬────┘
 *       是  │    否
 *           │    │
 *           ▼    │
 *   ┌─────────────────┐    │
 *   │ 分配临时堆内存段 │    │
 *   └─────────────────┘    │
 *           │              │
 *           └──────┬───────┘
 *                  ▼
 *   ┌───────────────────────────┐
 *   │ copyRecordOrEvent 拷贝数据 │
 *   └───────────────────────────┘
 *                  │
 *                  ▼
 *   ┌───────────────────────────┐
 *   │ 封装为 BufferWithSubpartition │
 *   └───────────────────────────┘
 * </pre>
 *
 * <h2>部分记录处理</h2>
 *
 * <ul>
 *   <li>当 isPartialRecordAllowed=false 时，每个缓冲区只包含完整记录</li>
 *   <li>isLastBufferPartialRecord 用于跟踪上一个缓冲区是否以部分记录结束</li>
 *   <li>如果当前缓冲区的记录边界清晰，标记为 DATA_BUFFER_WITH_CLEAR_END</li>
 * </ul>
 */
public class TieredStorageSortBuffer extends SortBuffer {

    // 是否允许记录跨缓冲区拆分
    // false: 每个缓冲区只包含完整记录，便于下游解析
    // true:  记录可以跨缓冲区，节省空间但解析复杂
    private final boolean isPartialRecordAllowed;

    // 上一个输出缓冲区是否以部分记录结束
    // 用于控制当前缓冲区的起始读取位置和 DataType 标记
    private boolean isLastBufferPartialRecord;

    public TieredStorageSortBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            boolean isPartialRecordAllowed) {
        super(
                freeSegments,
                bufferRecycler,
                numSubpartitions,
                bufferSize,
                numGuaranteedBuffers,
                null);
        this.isPartialRecordAllowed = isPartialRecordAllowed;
        this.isLastBufferPartialRecord = false;
    }

    /**
     * 从排序缓冲区获取下一个已排序的数据缓冲区。
     *
     * <p>与父类实现的主要区别：
     * <ul>
     *   <li>支持部分记录控制：可配置是否允许记录跨缓冲区</li>
     *   <li>缓冲区类型标记：当不允许部分记录且记录边界清晰时，标记为 DATA_BUFFER_WITH_CLEAR_END</li>
     *   <li>事件特殊处理：为事件分配独立的堆内存段，确保事件独占一个缓冲区</li>
     * </ul>
     *
     * @param transitBuffer 用于存放读取数据的目标缓冲区
     * @return 包含数据和目标子分区ID的缓冲区对象，如果无数据则返回 null
     */
    @Override
    public BufferWithSubpartition getNextBuffer(@Nullable MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        // 无剩余数据时，将 transitBuffer 放入空闲队列并返回 null
        if (!hasRemaining()) {
            freeSegments.add(transitBuffer);
            return null;
        }

        int numBytesRead = 0;
        Buffer.DataType bufferDataType = Buffer.DataType.DATA_BUFFER;
        // 获取当前正在读取的子分区 ID
        int currentReadingSubpartitionId = subpartitionReadOrder[readOrderIndex];

        do {
            // Get the buffer index and offset from the index entry
            // 从索引项中解析缓冲区索引和偏移量
            int toReadBufferIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
            int toReadOffsetInBuffer = getSegmentOffsetFromPointer(readIndexEntryAddress);

            // Get the lengthAndDataType buffer according the buffer index
            // 获取存储索引信息的 MemorySegment
            MemorySegment toReadBuffer = segments.get(toReadBufferIndex);

            // From the lengthAndDataType buffer, read and get the length and the data type
            // 读取并解析 8 字节的 length+dataType 组合值
            long lengthAndDataType = toReadBuffer.getLong(toReadOffsetInBuffer);
            int recordLength = getSegmentIndexFromPointer(lengthAndDataType);
            Buffer.DataType dataType =
                    Buffer.DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

            // If the buffer is an event and some data has been read, return it directly to ensure
            // that the event will occupy one buffer independently
            // 遇到事件且已读取部分数据时，先返回已读数据，确保事件独占缓冲区
            if (dataType.isEvent() && numBytesRead > 0) {
                break;
            }
            bufferDataType = dataType;

            // Get the next index entry address and move the read position forward
            // 读取下一个索引项的地址，并移动当前读取偏移
            long nextReadIndexEntryAddress = toReadBuffer.getLong(toReadOffsetInBuffer + 8);
            toReadOffsetInBuffer += INDEX_ENTRY_SIZE;

            // Allocate a temp buffer for the event, recycle the original buffer
            // 为事件分配独立的堆内存段，将原 transitBuffer 归还空闲队列
            if (bufferDataType.isEvent()) {
                freeSegments.add(transitBuffer);
                transitBuffer = MemorySegmentFactory.allocateUnpooledSegment(recordLength);
            }

            // 不允许部分记录时，检查当前记录是否能完整放入缓冲区
            // 如果不能且上一个缓冲区没有部分记录，则先返回已读数据
            if (!isPartialRecordAllowed
                    && !isLastBufferPartialRecord
                    && numBytesRead > 0
                    && numBytesRead + recordLength > transitBuffer.size()) {
                break;
            }

            // Start reading data from the data buffer
            // 从数据区拷贝记录内容到 transitBuffer
            numBytesRead +=
                    copyRecordOrEvent(
                            transitBuffer,
                            numBytesRead,
                            toReadBufferIndex,
                            toReadOffsetInBuffer,
                            recordLength);

            // 检查当前记录是否完整读取
            if (recordRemainingBytes == 0) {
                // move to next subpartition if the current subpartition has been finished
                // 当前子分区读取完毕，切换到下一个子分区
                if (readIndexEntryAddress
                        == lastIndexEntryAddresses[currentReadingSubpartitionId]) {
                    isLastBufferPartialRecord = false;
                    updateReadSubpartitionAndIndexEntryAddress();
                    break;
                }
                // 移动到下一条记录
                readIndexEntryAddress = nextReadIndexEntryAddress;
                // 如果上一个缓冲区有部分记录，现在已经补齐，标记清除
                if (isLastBufferPartialRecord) {
                    isLastBufferPartialRecord = false;
                    break;
                }
            } else {
                // 记录未完整读取，标记为部分记录状态
                isLastBufferPartialRecord = true;
            }
        } while (numBytesRead < transitBuffer.size() && bufferDataType.isBuffer());

        // 不允许部分记录且当前缓冲区记录边界清晰时，标记为 CLEAR_END
        if (!isPartialRecordAllowed
                && !isLastBufferPartialRecord
                && bufferDataType == Buffer.DataType.DATA_BUFFER) {
            bufferDataType = Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END;
        }

        numTotalBytesRead += numBytesRead;
        // 构建输出缓冲区：事件使用 FreeingBufferRecycler（一次性堆内存），数据使用传入的回收器
        return new BufferWithSubpartition(
                new NetworkBuffer(
                        transitBuffer,
                        bufferDataType.isBuffer() ? bufferRecycler : FreeingBufferRecycler.INSTANCE,
                        bufferDataType,
                        numBytesRead),
                currentReadingSubpartitionId);
    }

    /**
     * 获取当前记录剩余未读取的字节数。
     *
     * <p>用于 SortBufferAccumulator 计算后续连续缓冲区数量。
     */
    int getRecordRemainingBytes() {
        return recordRemainingBytes;
    }
}
