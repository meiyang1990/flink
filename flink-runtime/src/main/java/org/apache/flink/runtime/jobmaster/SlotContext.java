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

import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;

/**
 * Interface for the context of a {@link LogicalSlot}. This context contains information about the
 * underlying allocated slot and how to communicate with the TaskManager on which it was allocated.
 *
 * <p>【学习型注释】
 * SlotContext 是 LogicalSlot 的上下文接口，提供了访问底层 Slot 信息和与 TaskManager 通信的能力。
 * 它继承自 SlotInfo，扩展了获取 TaskManagerGateway 的方法，使得上层组件（如 Execution）
 * 可以通过 LogicalSlot 与 TaskExecutor 进行 RPC 通信。
 * 这是连接调度层（Scheduler）和执行层（TaskExecutor）的关键抽象。
 */
public interface SlotContext extends SlotInfo {

    /**
     * Gets the actor gateway that can be used to send messages to the TaskManager.
     *
     * <p>This method should be removed once the new interface-based RPC abstraction is in place
     *
     * @return The gateway that can be used to send messages to the TaskManager.
     */
    TaskManagerGateway getTaskManagerGateway();
}
