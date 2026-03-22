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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 【中文说明】NettyPayload 表示通过 Netty 连接传输的数据单元。
 *
 * <p>它是一个多态容器，可以携带以下三类信息之一：
 * <ul>
 *   <li><b>数据缓冲区 (Buffer)</b>：携带具体的 Shuffle 缓冲区数据、缓冲区索引及子分区 ID。</li>
 *   <li><b>Segment 标识 (Segment ID)</b>：用于通知 Consumer 新的 Segment 开始。</li>
 *   <li><b>错误信息 (Throwable)</b>：通知 Consumer 发生了异常。</li>
 * </ul>
 */
public class NettyPayload {

    /** Shuffle 缓冲区数据。非空时，bufferIndex 和 subpartitionId 有效。 */
    @Nullable private final Buffer buffer;

    /** 错误信息。非空时，表示传输层发生异常。 */
    @Nullable private final Throwable error;

    /** 缓冲区在 Segment 内的顺序索引。 */
    private final int bufferIndex;

    /** 子分区 ID。 */
    private final int subpartitionId;

    /** Segment ID。非负数时表示这是一个 Segment 切换控制消息。 */
    private final int segmentId;

    private NettyPayload(
            @Nullable Buffer buffer,
            int bufferIndex,
            int subpartitionId,
            @Nullable Throwable error,
            int segmentId) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
        this.error = error;
        this.segmentId = segmentId;
    }

    /** 创建缓冲区类型的 Payload */
    public static NettyPayload newBuffer(Buffer buffer, int bufferIndex, int subpartitionId) {
        checkState(buffer != null && bufferIndex != -1 && subpartitionId != -1);
        return new NettyPayload(buffer, bufferIndex, subpartitionId, null, -1);
    }

    /** 创建错误类型的 Payload */
    public static NettyPayload newError(Throwable error) {
        return new NettyPayload(null, -1, -1, checkNotNull(error), -1);
    }

    /** 创建 Segment 切换类型的 Payload */
    public static NettyPayload newSegment(int segmentId) {
        checkState(segmentId != -1);
        return new NettyPayload(null, -1, -1, null, segmentId);
    }
    // ... (后续方法保持不变)

    public Optional<Buffer> getBuffer() {
        return buffer != null ? Optional.of(buffer) : Optional.empty();
    }

    public Optional<Throwable> getError() {
        return error != null ? Optional.of(error) : Optional.empty();
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getSubpartitionId() {
        return subpartitionId;
    }

    public int getSegmentId() {
        return segmentId;
    }
}
