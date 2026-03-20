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

/**
 * Interface for components that hold slots and to which slots get released / recycled.
 *
 * <p>【学习型注释】
 * SlotOwner 定义了 Slot 所有者的接口，负责接收和回收 LogicalSlot。
 * 当 Execution 完成或失败时，其占用的 LogicalSlot 需要被释放回 SlotOwner（通常是 SlotPool）。
 * SlotOwner 决定如何重新分配或回收该 Slot，可能：
 * - 将 Slot 重新分配给其他等待的 Execution
 * - 将 Slot 返回给 ResourceManager（如果不再需要）
 * - 保留 Slot 以供同一 SlotSharingGroup 的其他 Task 使用
 * 这是 Flink 资源管理中的关键回收机制。
 */
public interface SlotOwner {

    /**
     * Return the given slot to the slot owner.
     *
     * @param logicalSlot to return
     */
    void returnLogicalSlot(LogicalSlot logicalSlot);
}
