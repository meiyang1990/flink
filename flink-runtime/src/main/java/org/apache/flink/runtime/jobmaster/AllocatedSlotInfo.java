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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Information about an allocated slot which is owned by a JobMaster.
 *
 * <p>【学习型注释】
 * AllocatedSlotInfo 封装了 JobMaster 拥有的已分配 Slot 的基本信息。
 * 它用于在 AllocatedSlotReport 中传递 Slot 信息，包含：
 * - slotIndex: Slot 在 TaskManager 上的索引位置
 * - allocationId: Slot 分配的唯一标识符
 * 该类是可序列化的，便于在 JobMaster 和 TaskExecutor 之间传输。
 */
public class AllocatedSlotInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int slotIndex;

    private final AllocationID allocationId;

    public AllocatedSlotInfo(int index, AllocationID allocationId) {
        checkArgument(index >= 0);
        this.slotIndex = index;
        this.allocationId = checkNotNull(allocationId);
    }

    public AllocationID getAllocationId() {
        return allocationId;
    }

    public int getSlotIndex() {
        return slotIndex;
    }

    @Override
    public String toString() {
        return "AllocatedSlotInfo{"
                + "slotIndex="
                + slotIndex
                + ", allocationId="
                + allocationId
                + '}';
    }
}
