/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Interface that provides basic information in the context of a slot.
 *
 * <p>【学习型注释】
 * SlotInfo 提供了 Slot 的基本信息接口，是 SlotContext 的父接口。
 * 包含的关键信息：
 * - AllocationID: Slot 分配的唯一标识
 * - TaskManagerLocation: TaskManager 的位置信息（主机、端口等）
 * - PhysicalSlotNumber: Slot 在 TaskManager 上的物理编号
 * - ResourceProfile: Slot 的资源配置（CPU、内存等）
 * 该接口被 LogicalSlot 和 PhysicalSlot 实现，为调度器提供统一的 Slot 信息访问方式。
 */
public interface SlotInfo {

    /**
     * Gets the id under which the slot has been allocated on the TaskManager. This id uniquely
     * identifies the physical slot.
     *
     * @return The id under which the slot has been allocated on the TaskManager
     */
    AllocationID getAllocationId();

    /**
     * Gets the location info of the TaskManager that offers this slot.
     *
     * @return The location info of the TaskManager that offers this slot
     */
    TaskManagerLocation getTaskManagerLocation();

    /**
     * Gets the number of the slot.
     *
     * @return The number of the slot on the TaskManager.
     */
    int getPhysicalSlotNumber();

    /**
     * Returns the resource profile of the slot.
     *
     * @return the resource profile of the slot.
     */
    ResourceProfile getResourceProfile();

    /**
     * Returns whether the slot will be occupied indefinitely.
     *
     * @return true if the slot will be occupied indefinitely, otherwise false.
     */
    boolean willBeOccupiedIndefinitely();
}
