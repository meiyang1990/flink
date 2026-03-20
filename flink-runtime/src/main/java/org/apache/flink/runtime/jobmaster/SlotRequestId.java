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

import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.util.AbstractID;

/**
 * This ID identifies the request for a slot from the Execution to the {@link SlotPool} or {@link
 * PhysicalSlotProvider}. There are various slot types like {@link PhysicalSlot}, {@link
 * LogicalSlot} or {@code SharedSlot} in the case of slot sharing.
 *
 * <p>This ID serves a different purpose than the {@link
 * org.apache.flink.runtime.clusterframework.types.AllocationID AllocationID}, which identifies the
 * request of a physical slot, issued from the SlotPool via the ResourceManager to the TaskManager.
 *
 * <p>【学习型注释】
 * SlotRequestId 标识从 Execution 到 SlotPool 或 PhysicalSlotProvider 的 Slot 请求。
 * 与 AllocationID 的区别：
 * - SlotRequestId: Execution 向 SlotPool 请求 Slot 时使用，属于逻辑层
 * - AllocationID: SlotPool 向 ResourceManager 请求物理 Slot 时使用，属于物理层
 * 一个 SlotRequestId 可能对应多个 AllocationID（Slot Sharing 场景下多个 Task 共享 Slot）。
 * 该 ID 继承自 AbstractID，提供全局唯一的标识能力。
 */
public final class SlotRequestId extends AbstractID {

    private static final long serialVersionUID = -6072105912250154283L;

    public SlotRequestId(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    public SlotRequestId() {}

    @Override
    public String toString() {
        return "SlotRequestId{" + super.toString() + '}';
    }
}
