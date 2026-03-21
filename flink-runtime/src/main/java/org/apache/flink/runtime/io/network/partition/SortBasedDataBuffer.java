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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * When getting buffers, The {@link SortBasedDataBuffer} should recycle the read target buffer with
 * the given {@link BufferRecycler}.
 *
 * <h2>核心设计概述</h2>
 * <p>SortBasedDataBuffer 是 {@link SortBuffer} 的具体实现类，提供了 getNextBuffer 方法的实现，
 * 用于按子分区顺序读取数据并拷贝到目标缓冲区。
 *
 * <h3>1. 读取流程</h3>
 * <pre>
 *   getNextBuffer 调用流程：
 *   1. 从 readIndexEntryAddress 读取当前索引项（长度 + 类型 + 下一地址）
 *   2. 调用 copyRecordOrEvent 将数据拷贝到 transitBuffer
 *   3. 循环拷贝直到：
 *      - transitBuffer 满
 *      - 遇到事件（事件必须单独返回）
 *      - 当前子分区数据读完
 *   4. 返回封装好的 BufferWithSubpartition
 * </pre>
 *
 * <h3>2. 事件处理</h3>
 * <ul>
 *   <li>事件（如 Checkpoint Barrier）必须单独返回，不能与普通数据混合</li>
 *   <li>如果事件大小超过 transitBuffer，会临时分配更大的堆外内存</li>
 *   <li>遇到事件时立即中断当前批次读取，确保事件边界清晰</li>
 * </ul>
 *
 * <h3>3. 使用场景</h3>
 * <ul>
 *   <li>{@link SortMergeResultPartition} 的 broadcast 数据缓冲</li>
 *   <li>需要高内存利用率的场景（无内部碎片）</li>
 * </ul>
 *
 * <h3>4. 线程安全</h3>
 * <p>非线程安全（标注 @NotThreadSafe），由调用者保证单线程访问。
 *
 * @see SortBuffer
 * @see HashBasedDataBuffer
 * @see SortMergeResultPartition
 */
@NotThreadSafe
public class SortBasedDataBuffer extends SortBuffer {

    public SortBasedDataBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        super(
                freeSegments,
                bufferRecycler,
                numSubpartitions,
                bufferSize,
                numGuaranteedBuffers,
                customReadOrder);
    }

    /**
     * 按子分区顺序读取下一个缓冲区数据。
     *
     * <p>核心读取方法，实现 Sort-Merge Shuffle 的"按子分区顺序输出"语义：
     *
     * <h4>读取策略</h4>
     * <ol>
     *   <li>从当前子分区的索引链表中读取索引项</li>
     *   <li>解析索引项获取记录长度和数据类型</li>
     *   <li>将数据拷贝到 transitBuffer，尽可能填满</li>
     *   <li>遇到事件时立即中断，单独返回事件</li>
     *   <li>当前子分区读完后切换到下一个子分区</li>
     * </ol>
     *
     * <h4>事件处理</h4>
     * <ul>
     *   <li>如果已有数据被拷贝，遇到事件时先返回已拷贝的数据</li>
     *   <li>事件单独返回，保证事件边界清晰</li>
     *   <li>事件大小超过 transitBuffer 时，临时分配更大的堆外内存</li>
     * </ul>
     *
     * @param transitBuffer 用于承载读取数据的目标内存段
     * @return 包含缓冲区数据和子分区索引的封装对象，无更多数据时返回 null
     */
    @Override
    public BufferWithSubpartition getNextBuffer(MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        if (!hasRemaining()) {
            return null;
        }

        int numBytesCopied = 0;
        DataType bufferDataType = DataType.DATA_BUFFER;
        int subpartitionIndex = subpartitionReadOrder[readOrderIndex];

        do {
            int sourceSegmentIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
            int sourceSegmentOffset = getSegmentOffsetFromPointer(readIndexEntryAddress);
            MemorySegment sourceSegment = segments.get(sourceSegmentIndex);

            long lengthAndDataType = sourceSegment.getLong(sourceSegmentOffset);
            int length = getSegmentIndexFromPointer(lengthAndDataType);
            DataType dataType = DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

            // return the data read directly if the next to read is an event
            if (dataType.isEvent() && numBytesCopied > 0) {
                break;
            }
            bufferDataType = dataType;

            // get the next index entry address and move the read position forward
            long nextReadIndexEntryAddress = sourceSegment.getLong(sourceSegmentOffset + 8);
            sourceSegmentOffset += INDEX_ENTRY_SIZE;

            // allocate a temp buffer for the event if the target buffer is not big enough
            if (bufferDataType.isEvent() && transitBuffer.size() < length) {
                transitBuffer = MemorySegmentFactory.allocateUnpooledSegment(length);
            }

            numBytesCopied +=
                    copyRecordOrEvent(
                            transitBuffer,
                            numBytesCopied,
                            sourceSegmentIndex,
                            sourceSegmentOffset,
                            length);

            if (recordRemainingBytes == 0) {
                // move to next subpartition if the current subpartition has been finished
                if (readIndexEntryAddress == lastIndexEntryAddresses[subpartitionIndex]) {
                    updateReadSubpartitionAndIndexEntryAddress();
                    break;
                }
                readIndexEntryAddress = nextReadIndexEntryAddress;
            }
        } while (numBytesCopied < transitBuffer.size() && bufferDataType.isBuffer());

        numTotalBytesRead += numBytesCopied;
        Buffer buffer =
                new NetworkBuffer(transitBuffer, (buf) -> {}, bufferDataType, numBytesCopied);
        return new BufferWithSubpartition(buffer, subpartitionIndex);
    }
}
