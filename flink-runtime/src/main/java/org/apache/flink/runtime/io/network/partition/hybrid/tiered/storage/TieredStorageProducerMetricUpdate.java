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

/**
 * The metric statistics for the tiered storage producer.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageProducerMetricUpdate 封装生产者端的增量指标数据，用于汇报给
 * TieredResultPartition 进行指标统计更新。
 *
 * <h2>指标说明</h2>
 *
 * <ul>
 *   <li><b>numWriteBuffersDelta</b>: 本次写入的缓冲区数量增量</li>
 *   <li><b>numWriteBytesDelta</b>: 本次写入的字节数增量</li>
 * </ul>
 *
 * <h2>使用流程</h2>
 *
 * <pre>
 *   TieredStorageProducerClient.write()
 *           │
 *           │ 数据写入完成
 *           ▼
 *   new TieredStorageProducerMetricUpdate(buffersDelta, bytesDelta)
 *           │
 *           │ 返回给调用方
 *           ▼
 *   TieredResultPartition.updateProducerMetricStatistics()
 *           │
 *           │ 更新全局计数器
 *           ▼
 *   numBuffersOut += buffersDelta
 *   numBytesOut += bytesDelta
 * </pre>
 */
public class TieredStorageProducerMetricUpdate {

    // 写入的缓冲区数量增量
    private final int numWriteBuffersDelta;

    // 写入的字节数增量
    private final int numWriteBytesDelta;

    TieredStorageProducerMetricUpdate(int numWriteBuffersDelta, int numWriteBytesDelta) {
        this.numWriteBuffersDelta = numWriteBuffersDelta;
        this.numWriteBytesDelta = numWriteBytesDelta;
    }

    /**
     * 获取写入的缓冲区数量增量。
     */
    public int numWriteBuffersDelta() {
        return numWriteBuffersDelta;
    }

    /**
     * 获取写入的字节数增量。
     */
    public int numWriteBytesDelta() {
        return numWriteBytesDelta;
    }
}
