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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.util.AutoCloseableAsync;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * Container for multiple {@link TaskSlot} instances. Additionally, it maintains multiple indices
 * for faster access to tasks and sets of allocated slots.
 *
 * <p>The task slot table automatically registers timeouts for allocated slots which cannot be
 * assigned to a job manager.
 *
 * <p>Before the task slot table can be used, it must be started via the {@link #start} method.
 *
 * <p>TaskSlot 表，管理 TaskExecutor 上所有 TaskSlot 的容器。
 *
 * <p>核心概念：
 * <ul>
 *     <li><b>TaskSlot</b>：Task 运行的逻辑单元，持有一定的资源（CPU、内存等）</li>
 *     <li><b>AllocationID</b>：Slot 分配的唯一标识，由 JobManager 生成</li>
 *     <li><b>索引结构</b>：维护多个索引以支持快速查询（按 Job、按 Allocation、按 Task）</li>
 * </ul>
 *
 * <p>Slot 状态机：
 * <pre>
 *     FREE ──allocateSlot()──> ALLOCATED ──markSlotActive()──> ACTIVE
 *       ^                          │                            │
 *       │                          │                            │
 *       │                    freeSlot()                   markSlotInactive()
 *       │                          │                            │
 *       │                          v                            v
 *       └────────────────── RELEASING <─────────────────────────┘
 * </pre>
 *
 * <p>核心功能：
 * <ul>
 *     <li><b>Slot 分配</b>：{@link #allocateSlot} 将 Slot 分配给特定 Job</li>
 *     <li><b>状态管理</b>：{@link #markSlotActive}/{@link #markSlotInactive} 管理活跃状态</li>
 *     <li><b>Task 管理</b>：{@link #addTask}/{@link #removeTask} 管理 Slot 中的 Task</li>
 *     <li><b>超时处理</b>：继承 {@link TimeoutListener}，处理 Slot 分配超时</li>
 *     <li><b>状态报告</b>：{@link #createSlotReport} 生成向 ResourceManager 汇报的 Slot 状态</li>
 * </ul>
 *
 * <p>动态 Slot 分配：
 * <ul>
 *     <li>传统模式：Slot 数量固定，由配置决定</li>
 *     <li>Fine-grained 模式：支持动态创建 Slot，index 为负数时自动分配</li>
 * </ul>
 *
 * <p>线程安全：所有方法都在 TaskExecutor 主线程执行，通过 {@link ComponentMainThreadExecutor} 保证
 *
 * @param <T> Slot 中 Task 的类型，必须实现 {@link TaskSlotPayload}
 */
public interface TaskSlotTable<T extends TaskSlotPayload>
        extends TimeoutListener<AllocationID>, AutoCloseableAsync {
    /**
     * Start the task slot table with the given slot actions.
     *
     * <p>启动 TaskSlot 表，必须在使用其他方法前调用。
     *
     * @param initialSlotActions to use for slot actions
     * @param mainThreadExecutor {@link ComponentMainThreadExecutor} to schedule internal calls to
     *     the main thread
     */
    void start(SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor);

    /**
     * Returns the all {@link AllocationID} for the given job.
     *
     * <p>获取指定 Job 的所有 Slot 分配 ID。
     *
     * @param jobId for which to return the set of {@link AllocationID}.
     * @return Set of {@link AllocationID} for the given job
     */
    Set<AllocationID> getAllocationIdsPerJob(JobID jobId);

    /**
     * Returns the {@link AllocationID} of any active task listed in this {@code TaskSlotTable}.
     *
     * <p>获取所有活跃 Task 所在 Slot 的分配 ID。
     *
     * @return The {@code AllocationID} of any active task.
     */
    Set<AllocationID> getActiveTaskSlotAllocationIds();

    /**
     * Returns the {@link AllocationID} of active {@link TaskSlot}s attached to the job with the
     * given {@link JobID}.
     *
     * <p>获取指定 Job 的所有活跃 Slot 的分配 ID。
     *
     * @param jobId The {@code JobID} of the job for which the {@code AllocationID}s of the attached
     *     active {@link TaskSlot}s shall be returned.
     * @return A set of {@code AllocationID}s that belong to active {@code TaskSlot}s having the
     *     passed {@code JobID}.
     */
    Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId);

    /**
     * 创建 Slot 状态报告，用于向 ResourceManager 汇报当前 TaskExecutor 的 Slot 状态。
     */
    SlotReport createSlotReport(ResourceID resourceId);

    /**
     * Allocate the slot with the given index for the given job and allocation id. If negative index
     * is given, a new auto increasing index will be generated. Returns true if the slot could be
     * allocated. Otherwise it returns false.
     *
     * <p>分配指定索引的 Slot 给特定 Job。
     * <ul>
     *     <li>index >= 0：分配指定索引的静态 Slot</li>
     *     <li>index < 0：动态分配新 Slot（Fine-grained 模式）</li>
     * </ul>
     *
     * @param index of the task slot to allocate, use negative value for dynamic slot allocation
     * @param jobId to allocate the task slot for
     * @param allocationId identifying the allocation
     * @param slotTimeout until the slot times out
     * @throws SlotAllocationException if allocating the slot failed.
     */
    @VisibleForTesting
    void allocateSlot(int index, JobID jobId, AllocationID allocationId, Duration slotTimeout)
            throws SlotAllocationException;

    /**
     * Allocate the slot with the given index for the given job and allocation id. If negative index
     * is given, a new auto increasing index will be generated. Returns true if the slot could be
     * allocated. Otherwise it returns false.
     *
     * <p>带资源配置文件的 Slot 分配，用于 Fine-grained 资源管理。
     *
     * @param index of the task slot to allocate, use negative value for dynamic slot allocation
     * @param jobId to allocate the task slot for
     * @param allocationId identifying the allocation
     * @param resourceProfile of the requested slot, used only for dynamic slot allocation and will
     *     be ignored otherwise
     * @param slotTimeout until the slot times out
     * @throws SlotAllocationException if allocating the slot failed.
     */
    void allocateSlot(
            int index,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            Duration slotTimeout)
            throws SlotAllocationException;

    /**
     * Marks the slot under the given allocation id as active. If the slot could not be found, then
     * a {@link SlotNotFoundException} is thrown.
     *
     * <p>将 Slot 标记为活跃状态，表示有 Task 正在运行。
     * Slot 变为活跃后不会触发超时。
     *
     * @param allocationId to identify the task slot to mark as active
     * @throws SlotNotFoundException if the slot could not be found for the given allocation id
     * @return True if the slot could be marked active; otherwise false
     */
    boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException;

    /**
     * Marks the slot under the given allocation id as inactive. If the slot could not be found,
     * then a {@link SlotNotFoundException} is thrown.
     *
     * <p>将 Slot 标记为非活跃状态，Slot 会进入超时倒计时。
     * 超时后若仍未被使用，会触发 SlotActions.freeSlot()。
     *
     * @param allocationId to identify the task slot to mark as inactive
     * @param slotTimeout until the slot times out
     * @throws SlotNotFoundException if the slot could not be found for the given allocation id
     * @return True if the slot could be marked inactive
     */
    boolean markSlotInactive(AllocationID allocationId, Duration slotTimeout)
            throws SlotNotFoundException;

    /**
     * Try to free the slot. If the slot is empty it will set the state of the task slot to free and
     * return its index. If the slot is not empty, then it will set the state of the task slot to
     * releasing, fail all tasks and return -1.
     *
     * <p>释放 Slot：
     * <ul>
     *     <li>Slot 为空：直接释放，返回 Slot 索引</li>
     *     <li>Slot 非空：标记为 releasing，失败所有 Task，返回 -1</li>
     * </ul>
     *
     * @param allocationId identifying the task slot to be freed
     * @throws SlotNotFoundException if there is not task slot for the given allocation id
     * @return Index of the freed slot if the slot could be freed; otherwise -1
     */
    default int freeSlot(AllocationID allocationId) throws SlotNotFoundException {
        return freeSlot(allocationId, new Exception("The task slot of this task is being freed."));
    }

    /**
     * Tries to free the slot. If the slot is empty it will set the state of the task slot to free
     * and return its index. If the slot is not empty, then it will set the state of the task slot
     * to releasing, fail all tasks and return -1.
     *
     * <p>带失败原因的 Slot 释放，原因会传递给 Slot 中的所有 Task。
     *
     * @param allocationId identifying the task slot to be freed
     * @param cause to fail the tasks with if slot is not empty
     * @throws SlotNotFoundException if there is not task slot for the given allocation id
     * @return Index of the freed slot if the slot could be freed; otherwise -1
     */
    int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException;

    /**
     * Check whether the timeout with ticket is valid for the given allocation id.
     *
     * <p>验证超时票据是否有效。每次 Slot 状态变化会生成新票据，
     * 旧票据对应的超时回调会被忽略。
     *
     * @param allocationId to check against
     * @param ticket of the timeout
     * @return True if the timeout is valid; otherwise false
     */
    boolean isValidTimeout(AllocationID allocationId, UUID ticket);

    /**
     * Check whether the slot for the given index is allocated for the given job and allocation id.
     *
     * <p>检查指定 Slot 是否已分配给特定 Job 和 AllocationID。
     *
     * @param index of the task slot
     * @param jobId for which the task slot should be allocated
     * @param allocationId which should match the task slot's allocation id
     * @return True if the given task slot is allocated for the given job and allocation id
     */
    boolean isAllocated(int index, JobID jobId, AllocationID allocationId);

    /**
     * Try to mark the specified slot as active if it has been allocated by the given job.
     *
     * <p>尝试将 Slot 标记为活跃，仅当 Slot 属于指定 Job 时成功。
     *
     * @param jobId of the allocated slot
     * @param allocationId identifying the allocation
     * @return True if the task slot could be marked active.
     */
    boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId);

    /**
     * Check whether the task slot with the given index is free.
     *
     * <p>检查指定索引的 Slot 是否空闲。
     *
     * @param index of the task slot
     * @return True if the task slot is free; otherwise false
     */
    boolean isSlotFree(int index);

    /**
     * Check whether the job has allocated (not active) slots.
     *
     * <p>检查 Job 是否有已分配但非活跃的 Slot。
     *
     * @param jobId for which to check for allocated slots
     * @return True if there are allocated slots for the given job id.
     */
    boolean hasAllocatedSlots(JobID jobId);

    /**
     * Return an iterator of allocated slots for the given job id.
     *
     * <p>获取指定 Job 的所有已分配 Slot 迭代器。
     *
     * @param jobId for which to return the allocated slots
     * @return Iterator of allocated slots.
     */
    Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId);

    /**
     * Returns the owning job of the {@link TaskSlot} identified by the given {@link AllocationID}.
     *
     * <p>获取 Slot 所属的 Job。
     *
     * @param allocationId identifying the slot for which to retrieve the owning job
     * @return Owning job of the specified {@link TaskSlot} or null if there is no slot for the
     *     given allocation id or if the slot has no owning job assigned
     */
    @Nullable
    JobID getOwningJob(AllocationID allocationId);

    /**
     * Add the given task to the slot identified by the task's allocation id.
     *
     * <p>将 Task 添加到其对应的 Slot 中。
     * Slot 必须存在且处于活跃状态。
     *
     * @param task to add to the task slot with the respective allocation id
     * @throws SlotNotFoundException if there was no slot for the given allocation id
     * @throws SlotNotActiveException if there was no slot active for task's job and allocation id
     * @return True if the task could be added to the task slot; otherwise false
     */
    boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException;

    /**
     * Remove the task with the given execution attempt id from its task slot. If the owning task
     * slot is in state releasing and empty after removing the task, the slot is freed via the slot
     * actions.
     *
     * <p>从 Slot 中移除 Task。
     * 如果 Slot 处于 releasing 状态且移除后为空，会触发 Slot 释放。
     *
     * @param executionAttemptID identifying the task to remove
     * @return The removed task if there is any for the given execution attempt id; otherwise null
     */
    T removeTask(ExecutionAttemptID executionAttemptID);

    /**
     * Get the task for the given execution attempt id. If none could be found, then return null.
     *
     * <p>根据执行尝试 ID 获取 Task。
     *
     * @param executionAttemptID identifying the requested task
     * @return The task for the given execution attempt id if it exist; otherwise null
     */
    T getTask(ExecutionAttemptID executionAttemptID);

    /**
     * Return an iterator over all tasks for a given job.
     *
     * <p>获取指定 Job 的所有 Task 迭代器。
     *
     * @param jobId identifying the job of the requested tasks
     * @return Iterator over all task for a given job
     */
    Iterator<T> getTasks(JobID jobId);

    /**
     * Get the current allocation for the task slot with the given index.
     *
     * <p>获取指定索引 Slot 当前的 AllocationID。
     *
     * @param index identifying the slot for which the allocation id shall be retrieved
     * @return Allocation id of the specified slot if allocated; otherwise null
     */
    AllocationID getCurrentAllocation(int index);

    /**
     * Get the memory manager of the slot allocated for the task.
     *
     * <p>获取 Slot 的内存管理器，用于 Task 的托管内存分配。
     *
     * @param allocationID allocation id of the slot allocated for the task
     * @return the memory manager of the slot allocated for the task
     */
    MemoryManager getTaskMemoryManager(AllocationID allocationID) throws SlotNotFoundException;
}
