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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/**
 * 【中文说明】NettyConnectionReader 是用于从 Netty Shuffle 传输连接中读取缓冲区的接口。
 *
 * <p>核心职责：
 * <ul>
 *   <li>由 TierConsumerAgent 调用，负责处理底层 Netty 传输的缓冲区接收。</li>
 *   <li>提供缓冲区读取状态探测，支持按子分区定位数据。</li>
 * </ul>
 */
public interface NettyConnectionReader {
    /**
     * 【中文说明】探测下一个有数据可读的子分区 ID。
     *
     * @return 下一个可读取的子分区 ID，若暂无缓冲区可读则返回 -1。
     */
    int peekNextBufferSubpartitionId() throws IOException;

    /**
     * 【中文说明】从 Netty 连接中读取指定子分区和 Segment 的缓冲区。
     *
     * @param subpartitionId 子分区 ID
     * @param segmentId Segment ID
     * @return 读取到的缓冲区（可能为空）
     */
    Optional<Buffer> readBuffer(int subpartitionId, int segmentId);
}
