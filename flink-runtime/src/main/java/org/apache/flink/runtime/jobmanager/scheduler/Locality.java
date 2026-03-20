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

package org.apache.flink.runtime.jobmanager.scheduler;

/**
 * 【学习型注释】
 * Locality 枚举定义了 Task 调度时的数据本地性级别。
 * 数据本地性是大数据计算框架的重要优化手段，尽量让计算靠近数据，减少网络传输：
 * - UNCONSTRAINED: 无约束，可调度到任意位置
 * - LOCAL: 最理想情况，Task 与数据在同一 TaskManager 进程内
 * - HOST_LOCAL: 较好情况，Task 与数据在同一物理主机上
 * - NON_LOCAL: 较差情况，Task 被调度到了偏好位置之外
 * - UNKNOWN: 未知状态，没有提供本地性信息
 */
public enum Locality {

    /** No constraint existed on the task placement. */
    UNCONSTRAINED,

    /** The task was scheduled into the same TaskManager as requested */
    LOCAL,

    /** The task was scheduled onto the same host as requested */
    HOST_LOCAL,

    /** The task was scheduled to a destination not included in its locality preferences. */
    NON_LOCAL,

    /** No locality information was provided, it is unknown if the locality was respected */
    UNKNOWN
}
