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

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * {@link FileDataIndexRegionHelper} is responsible for writing a {@link Region} to the file or
 * reading a {@link Region} from file.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>该接口定义了索引区域（Region）的序列化和反序列化策略，是索引持久化的核心抽象。
 * 不同的索引实现可以有不同的 Region 格式，通过实现此接口来支持自定义的读写逻辑。
 *
 * <h2>职责分离</h2>
 *
 * <ul>
 *   <li><b>FileDataIndexCache</b>：负责内存缓存管理和 LRU 淘汰策略
 *   <li><b>FileDataIndexRegionHelper</b>：负责 Region 的序列化格式定义
 *   <li><b>FileDataIndexSpilledRegionManager</b>：负责磁盘文件的组织结构
 * </ul>
 *
 * @param <T> Region 的具体类型
 */
public interface FileDataIndexRegionHelper<T extends FileDataIndexRegionHelper.Region> {

    /**
     * 将索引区域写入文件。
     *
     * @param channel the file channel to write the region
     * @param region the region to be written to the file
     */
    void writeRegionToFile(FileChannel channel, T region) throws IOException;

    /**
     * 从文件中读取一个索引区域。
     *
     * @param channel the file channel to read the region
     * @param fileOffset the current region data is from this file offset, so start reading the file
     *     from the offset when reading the region
     * @return the region read from the file
     */
    T readRegionFromFile(FileChannel channel, long fileOffset) throws IOException;

    /**
     * 索引区域接口：表示一组在文件中物理连续的 buffer 集合。
     *
     * <h3>Region 的定义</h3>
     *
     * <p>一个 Region 代表一系列满足以下条件的 buffer：
     * <ul>
     *   <li>来自同一个子分区
     *   <li>逻辑上（buffer index）连续
     *   <li>物理上（文件偏移）连续
     * </ul>
     *
     * <h3>示例图解</h3>
     *
     * <p>以下示例展示了文件中物理连续的 buffer 及其对应的 Region，
     * 其中 x-y 表示来自子分区 x、buffer index 为 y 的 buffer，() 表示一个 Region：
     *
     * <pre>
     * (1-1, 1-2), (2-1), (2-2, 2-3), (1-5, 1-6), (1-4)
     * </pre>
     *
     * <p><b>注意事项：</b>
     * <ul>
     *   <li>文件中可能不包含所有 buffer（如上例中缺少 1-3）
     *   <li>文件中 buffer 的顺序可能与其 index 不同（如 1-4 在 1-6 之后）
     *   <li>索引不总是维护最长可能的 Region（如 2-1, 2-2, 2-3 被分成了两个 Region）
     * </ul>
     */
    interface Region {

        /** 获取该 Region 的总字节大小（包括头部字段和 buffer 数据）。 */
        int getSize();

        /** 获取该 Region 的首个 buffer index。 */
        int getFirstBufferIndex();

        /** 获取该 Region 在文件中的起始偏移量。 */
        long getRegionStartOffset();

        /** 获取该 Region 在文件中的结束偏移量。 */
        long getRegionEndOffset();

        /** 获取该 Region 包含的 buffer 数量。 */
        int getNumBuffers();

        /**
         * 判断当前 Region 是否包含指定的 buffer。
         *
         * @param bufferIndex the specific buffer index
         */
        boolean containBuffer(int bufferIndex);
    }
}
