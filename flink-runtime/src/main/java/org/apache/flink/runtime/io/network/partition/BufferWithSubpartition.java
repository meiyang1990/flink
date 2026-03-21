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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Buffer and the corresponding subpartition index.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>这是一个简单的数据封装类，将 {@link Buffer} 与其所属的子分区索引绑定在一起。
 * 在 Shuffle 过程中，一个 Buffer 需要被路由到特定的下游子分区，此类提供了这种关联关系的封装。
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>Sort-Merge Shuffle：在排序合并写入时，需要知道每个 Buffer 属于哪个子分区
 *   <li>Tiered Storage：分层存储中，Buffer 需要根据子分区进行分组和路由
 *   <li>数据分发：在数据从上游分发到下游时，携带子分区信息用于正确路由
 * </ul>
 *
 * <h2>不可变性</h2>
 *
 * <p>该类是不可变的（immutable），一旦创建，其 Buffer 和子分区索引都不可更改。
 * 这保证了在多线程环境下的安全性。
 */
public class BufferWithSubpartition {

    // ================== 核心数据字段 ==================

    /** 封装的数据缓冲区，包含实际的序列化数据或事件。 */
    private final Buffer buffer;

    /** 该缓冲区所属的目标子分区索引，范围为 [0, numSubpartitions)。 */
    private final int subpartitionIndex;

    public BufferWithSubpartition(Buffer buffer, int subpartitionIndex) {
        this.buffer = checkNotNull(buffer);
        this.subpartitionIndex = subpartitionIndex;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public int getSubpartitionIndex() {
        return subpartitionIndex;
    }
}
