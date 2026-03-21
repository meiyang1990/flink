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
 * The memory specs for a memory owner, including the owner itself, the number of guaranteed buffers
 * of the memory owner, etc.
 *
 * <h2>核心设计概述</h2>
 *
 * <p>TieredStorageMemorySpec 定义了内存使用者的内存规格配置，包括：
 *
 * <ul>
 *   <li><b>owner</b>: 内存使用者对象（如 BufferAccumulator、TierProducerAgent 等）</li>
 *   <li><b>numGuaranteedBuffers</b>: 保证分配给该 owner 的缓冲区数量</li>
 *   <li><b>guaranteedReclaimable</b>: 这些缓冲区是否可以被强制回收</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <pre>
 *   TieredStorageMemoryManagerImpl.setup()
 *           │
 *           │ 遍历所有 MemorySpec
 *           ▼
 *   ┌─────────────────────────────────────────┐
 *   │ 计算 maxNonReclaimableBuffers           │
 *   │ (不可回收缓冲区的上限)                    │
 *   │                                         │
 *   │ = Σ (guaranteedReclaimable=false 的     │
 *   │      numGuaranteedBuffers)               │
 *   └─────────────────────────────────────────┘
 * </pre>
 *
 * <h2>可回收性说明</h2>
 *
 * <ul>
 *   <li><b>可回收 (guaranteedReclaimable=true)</b>: 缓冲区可随时被回收，
 *       即使下游未及时消费。适用于可重新生成数据的场景（如磁盘层）</li>
 *   <li><b>不可回收 (guaranteedReclaimable=false)</b>: 缓冲区必须等待下游消费后才能回收。
 *       适用于数据不可重新生成的场景（如网络直传）</li>
 * </ul>
 */
public class TieredStorageMemorySpec {

    /** The memory use owner. */
    // 内存使用者对象，用于标识和追踪缓冲区归属
    private final Object owner;

    /** The number of guaranteed buffers of this memory owner. */
    // 保证分配给此 owner 的缓冲区数量
    private final int numGuaranteedBuffers;

    /**
     * Whether the buffers of this owner are guaranteed to be reclaimed, even if the downstream does
     * not consume them promptly.
     */
    // 缓冲区是否可被强制回收（即使下游未消费）
    // true: 可回收（如磁盘层，数据可从磁盘重新读取）
    // false: 不可回收（如直接网络传输，必须等待消费）
    private final boolean guaranteedReclaimable;

    public TieredStorageMemorySpec(Object owner, int numGuaranteedBuffers) {
        this(owner, numGuaranteedBuffers, true);
    }

    public TieredStorageMemorySpec(
            Object owner, int numGuaranteedBuffers, boolean guaranteedReclaimable) {
        this.owner = owner;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        this.guaranteedReclaimable = guaranteedReclaimable;
    }

    public Object getOwner() {
        return owner;
    }

    public int getNumGuaranteedBuffers() {
        return numGuaranteedBuffers;
    }

    public boolean isGuaranteedReclaimable() {
        return guaranteedReclaimable;
    }
}
