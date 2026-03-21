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
import org.apache.flink.runtime.clusterframework.types.ResourceBudgetManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor.DummyComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link TaskSlotTable}.
 *
 * <p>【学习型注释】TaskSlotTable 接口的默认实现，负责管理 TaskExecutor 上所有 Slot 的生命周期。
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li>Slot 分配与释放：管理静态 Slot（固定数量）和动态 Slot（按需分配）</li>
 *   <li>Task 到 Slot 的映射：跟踪每个 Task 运行在哪个 Slot 上</li>
 *   <li>Slot 状态机管理：ALLOCATED → ACTIVE → RELEASING</li>
 *   <li>超时管理：通过 TimerService 对已分配但未使用的 Slot 进行超时处理</li>
 *   <li>资源预算管理：通过 ResourceBudgetManager 确保资源分配不超限</li>
 * </ul>
 *
 * <h2>Slot 类型</h2>
 * <ul>
 *   <li>静态 Slot：index 在 [0, numberSlots) 范围内，数量固定</li>
 *   <li>动态 Slot：index >= numberSlots，按需动态分配（Fine-grained Resource Management）</li>
 * </ul>
 *
 * <h2>关键数据结构</h2>
 * <pre>
 * taskSlots:        index → TaskSlot           （所有 Slot）
 * allocatedSlots:   AllocationID → TaskSlot    （已分配 Slot 快速查找）
 * taskSlotMappings: ExecutionAttemptID → Task+Slot （Task 到 Slot 映射）
 * slotsPerJob:      JobID → Set<AllocationID>  （每个 Job 的 Slot 集合）
 * </pre>
 *
 * <h2>线程安全</h2>
 * 所有公共方法需通过 mainThreadExecutor 调度，确保在主线程执行。
 *
 * @param <T> TaskSlotPayload 类型，通常是 Task
 */
public class TaskSlotTableImpl<T extends TaskSlotPayload> implements TaskSlotTable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskSlotTableImpl.class);

    /**
     * 静态 Slot 的数量。静态 Slot 的 index 范围是 [0, numberSlots)。
     * 生成 SlotReport 时，即使某些 Slot 未实际创建，也需要报告这个范围内的所有 Slot。
     */
    private final int numberSlots;

    /**
     * 静态 Slot 的默认资源配置文件。当请求的 ResourceProfile 为 UNKNOWN 时使用此默认值。
     */
    private final ResourceProfile defaultSlotResourceProfile;

    /**
     * 内存页大小（字节），用于创建 Slot 关联的 MemoryManager。
     */
    private final int memoryPageSize;

    /**
     * 超时服务，用于对已分配但长时间未使用的 Slot 进行超时回收。
     * 当 Slot 处于 ALLOCATED 状态时注册超时，变为 ACTIVE 后取消超时。
     */
    private final TimerService<AllocationID> timerService;

    /**
     * 所有 TaskSlot 的索引映射表：slotIndex → TaskSlot。
     * 包含静态 Slot（index < numberSlots）和动态 Slot（index >= numberSlots）。
     */
    private final Map<Integer, TaskSlot<T>> taskSlots;

    /**
     * 已分配 Slot 的快速查找表：AllocationID → TaskSlot。
     * 支持通过 AllocationID 快速定位到对应的 Slot。
     */
    private final Map<AllocationID, TaskSlot<T>> allocatedSlots;

    /**
     * Task 与 Slot 的映射关系：ExecutionAttemptID → TaskSlotMapping。
     * TaskSlotMapping 封装了 Task 和其所在的 TaskSlot。
     */
    private final Map<ExecutionAttemptID, TaskSlotMapping<T>> taskSlotMappings;

    /**
     * 每个 Job 占用的 Slot 集合：JobID → Set<AllocationID>。
     * 用于 Job 级别的 Slot 查询和清理。
     */
    private final Map<JobID, Set<AllocationID>> slotsPerJob;

    /**
     * Slot 操作回调接口，用于通知外部组件（如 TaskExecutor）执行 Slot 释放或超时处理。
     */
    @Nullable private SlotActions slotActions;

    /**
     * 当前 TaskSlotTable 的状态：CREATED → RUNNING → CLOSING → CLOSED。
     * 使用 volatile 保证多线程可见性。
     */
    private volatile State state;

    /**
     * 动态 Slot 的下一个可用索引。始终 >= numberSlots，每次分配动态 Slot 后递增。
     */
    private int dynamicSlotIndex;

    /**
     * 资源预算管理器，跟踪和控制 TaskExecutor 上可用的总资源。
     * 分配 Slot 前需要 reserve，释放时需要 release。
     */
    private final ResourceBudgetManager budgetManager;

    /**
     * 关闭完成的 Future，当所有 Slot 释放完毕且状态变为 CLOSED 时完成。
     */
    private final CompletableFuture<Void> closingFuture;

    /**
     * 主线程执行器，所有状态变更操作需通过此执行器调度到主线程执行，保证线程安全。
     */
    private ComponentMainThreadExecutor mainThreadExecutor =
            new DummyComponentMainThreadExecutor(
                    "TaskSlotTableImpl is not initialized with proper main thread executor, "
                            + "call to TaskSlotTableImpl#start is required");

    /**
     * 后台执行器，用于执行耗时的后台任务，如验证托管内存是否完全释放。
     */
    private final Executor memoryVerificationExecutor;

    public TaskSlotTableImpl(
            final int numberSlots,
            final ResourceProfile totalAvailableResourceProfile,
            final ResourceProfile defaultSlotResourceProfile,
            final int memoryPageSize,
            final TimerService<AllocationID> timerService,
            final Executor memoryVerificationExecutor) {
        Preconditions.checkArgument(
                0 < numberSlots, "The number of task slots must be greater than 0.");

        this.numberSlots = numberSlots;
        this.dynamicSlotIndex = numberSlots;
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);
        this.memoryPageSize = memoryPageSize;

        this.taskSlots = CollectionUtil.newHashMapWithExpectedSize(numberSlots);

        this.timerService = Preconditions.checkNotNull(timerService);

        budgetManager =
                new ResourceBudgetManager(
                        Preconditions.checkNotNull(totalAvailableResourceProfile));

        allocatedSlots = CollectionUtil.newHashMapWithExpectedSize(numberSlots);

        taskSlotMappings = CollectionUtil.newHashMapWithExpectedSize(4 * numberSlots);

        slotsPerJob = CollectionUtil.newHashMapWithExpectedSize(4);

        slotActions = null;
        state = State.CREATED;
        closingFuture = new CompletableFuture<>();

        this.memoryVerificationExecutor = memoryVerificationExecutor;
    }

    @Override
    public void start(
            SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor) {
        Preconditions.checkState(
                state == State.CREATED,
                "The %s has to be just created before starting",
                TaskSlotTableImpl.class.getSimpleName());
        this.slotActions = Preconditions.checkNotNull(initialSlotActions);
        this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);

        timerService.start(this);

        state = State.RUNNING;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (state == State.CREATED) {
            state = State.CLOSED;
            closingFuture.complete(null);
        } else if (state == State.RUNNING) {
            state = State.CLOSING;
            final FlinkException cause = new FlinkException("Closing task slot table");
            CompletableFuture<Void> cleanupFuture =
                    FutureUtils.waitForAll(
                                    new ArrayList<>(allocatedSlots.values())
                                            .stream()
                                                    .map(slot -> freeSlotInternal(slot, cause))
                                                    .collect(Collectors.toList()))
                            .thenRunAsync(
                                    () -> {
                                        state = State.CLOSED;
                                        timerService.stop();
                                    },
                                    mainThreadExecutor);
            FutureUtils.forward(cleanupFuture, closingFuture);
        }
        return closingFuture;
    }

    @VisibleForTesting
    public boolean isClosed() {
        return state == State.CLOSED;
    }

    @Override
    public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
        final Set<AllocationID> allocationIds = slotsPerJob.get(jobId);

        if (allocationIds == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(allocationIds);
        }
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIds() {
        return createAllocationIdSet(new TaskSlotIterator(TaskSlotState.ACTIVE));
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId) {
        return createAllocationIdSet(new TaskSlotIterator(jobId, TaskSlotState.ACTIVE));
    }

    private Set<AllocationID> createAllocationIdSet(Iterator<TaskSlot<T>> taskSlotIterator) {
        Set<AllocationID> allocationIds = new HashSet<>();
        while (taskSlotIterator.hasNext()) {
            allocationIds.add(taskSlotIterator.next().getAllocationId());
        }

        return allocationIds;
    }

    // ---------------------------------------------------------------------
    // Slot report methods
    // ---------------------------------------------------------------------

    @Override
    public SlotReport createSlotReport(ResourceID resourceId) {
        List<SlotStatus> slotStatuses = new ArrayList<>();

        for (int i = 0; i < numberSlots; i++) {
            SlotID slotId = new SlotID(resourceId, i);
            SlotStatus slotStatus;
            if (taskSlots.containsKey(i)) {
                TaskSlot<T> taskSlot = taskSlots.get(i);

                slotStatus =
                        new SlotStatus(
                                slotId,
                                taskSlot.getResourceProfile(),
                                taskSlot.getJobId(),
                                taskSlot.getAllocationId(),
                                taskSlot.getAssignedTasks());
            } else {
                slotStatus = new SlotStatus(slotId, defaultSlotResourceProfile, null, null, 0);
            }

            slotStatuses.add(slotStatus);
        }

        for (TaskSlot<T> taskSlot : allocatedSlots.values()) {
            if (isDynamicIndex(taskSlot.getIndex())) {
                SlotStatus slotStatus =
                        new SlotStatus(
                                new SlotID(resourceId, taskSlot.getIndex()),
                                taskSlot.getResourceProfile(),
                                taskSlot.getJobId(),
                                taskSlot.getAllocationId(),
                                taskSlot.getAssignedTasks());
                slotStatuses.add(slotStatus);
            }
        }

        return new SlotReport(slotStatuses);
    }

    // ---------------------------------------------------------------------
    // Slot methods
    // ---------------------------------------------------------------------

    @VisibleForTesting
    @Override
    public void allocateSlot(
            int index, JobID jobId, AllocationID allocationId, Duration slotTimeout)
            throws SlotAllocationException {
        allocateSlot(index, jobId, allocationId, defaultSlotResourceProfile, slotTimeout);
    }

    /**
     * 分配一个 Slot 给指定的 Job。这是 Slot 生命周期的起点。
     *
     * <p>【学习型注释】Slot 分配流程：
     * <ol>
     *   <li>如果 requestedIndex < 0，表示动态 Slot 请求，分配一个 >= numberSlots 的索引</li>
     *   <li>检查 AllocationID 是否已分配（幂等处理，允许重复请求相同的 Slot）</li>
     *   <li>检查 index 是否已被占用（冲突检测）</li>
     *   <li>通过 budgetManager 预留资源，确保不超出总资源限制</li>
     *   <li>创建 TaskSlot 并注册到各映射表</li>
     *   <li>注册超时定时器（Slot 必须在超时前变为 ACTIVE 状态）</li>
     * </ol>
     */
    @Override
    public void allocateSlot(
            int requestedIndex,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            Duration slotTimeout)
            throws SlotAllocationException {
        // 检查 TaskSlotTable 是否处于 RUNNING 状态
        checkRunning();

        Preconditions.checkArgument(requestedIndex < numberSlots);

        // 负数 index 表示动态 Slot 请求，分配一个递增的动态索引
        // The negative requestIndex indicate that the SlotManager allocate a dynamic slot, we
        // transfer the index to an increasing number not less than the numberSlots.
        int index = requestedIndex < 0 ? nextDynamicSlotIndex() : requestedIndex;
        // 确定实际使用的资源配置文件
        ResourceProfile effectiveResourceProfile =
                resourceProfile.equals(ResourceProfile.UNKNOWN)
                        ? defaultSlotResourceProfile
                        : resourceProfile;

        // 幂等性检查：如果 AllocationID 已存在且参数一致，直接返回成功
        TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
        if (taskSlot != null) {
            if (isDuplicatedSlot(taskSlot, jobId, effectiveResourceProfile, index)) {
                LOG.info(
                        "Slot with allocationId {} already exist, with resource profile {}, job id {} and index {}. The required index is {}. No further allocation necessary.",
                        taskSlot.getAllocationId(),
                        taskSlot.getResourceProfile(),
                        taskSlot.getJobId(),
                        taskSlot.getIndex(),
                        index);
                return;
            }

            // AllocationID 已存在但参数不一致，属于冲突
            throw new SlotAllocationException(
                    String.format(
                            "A slot with allocationId %s and resource profile %s is already assigned to job %s with subtask index %d.",
                            taskSlot.getAllocationId(),
                            taskSlot.getResourceProfile(),
                            taskSlot.getJobId(),
                            taskSlot.getIndex()));
        } else if (isIndexAlreadyTaken(index)) {
            // index 已被另一个 Slot 占用
            throw new SlotAllocationException(
                    String.format(
                            "The slot with index %d is already assigned to another allocation with id %s.",
                            index, taskSlots.get(index).getAllocationId()));
        }

        // 预留资源，如果资源不足则抛出异常
        if (!budgetManager.reserve(effectiveResourceProfile)) {
            throw new SlotAllocationException(
                    String.format(
                            "Cannot allocate the requested resources. Trying to allocate %s, while the currently remaining available resources are %s, total is %s.",
                            effectiveResourceProfile,
                            budgetManager.getAvailableBudget(),
                            budgetManager.getTotalBudget()));
        }
        LOG.info(
                "Allocated slot for {} with resources {}.", allocationId, effectiveResourceProfile);

        // 创建新的 TaskSlot 实例
        taskSlot =
                new TaskSlot<>(
                        index,
                        effectiveResourceProfile,
                        memoryPageSize,
                        jobId,
                        allocationId,
                        memoryVerificationExecutor);
        taskSlots.put(index, taskSlot);

        // 更新 AllocationID 到 TaskSlot 的映射
        // update the allocation id to task slot map
        allocatedSlots.put(allocationId, taskSlot);

        // 注册超时定时器：Slot 必须在 slotTimeout 内变为 ACTIVE，否则被回收
        // register a timeout for this slot since it's in state allocated
        timerService.registerTimeout(allocationId, slotTimeout.toMillis(), TimeUnit.MILLISECONDS);

        // 维护 Job 到 Slot 的映射关系
        // add this slot to the set of job slots
        Set<AllocationID> slots = slotsPerJob.get(jobId);

        if (slots == null) {
            slots = CollectionUtil.newHashSetWithExpectedSize(4);
            slotsPerJob.put(jobId, slots);
        }

        slots.add(allocationId);
    }

    private boolean isDuplicatedSlot(
            TaskSlot taskSlot, JobID jobId, ResourceProfile resourceProfile, int index) {
        return taskSlot.getJobId().equals(jobId)
                && taskSlot.getResourceProfile().equals(resourceProfile)
                && (isDynamicIndex(index) || taskSlot.getIndex() == index);
    }

    private boolean isIndexAlreadyTaken(int index) {
        return taskSlots.get(index) != null;
    }

    private boolean isDynamicIndex(int index) {
        return index >= numberSlots;
    }

    @Override
    public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
        checkRunning();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return markExistingSlotActive(taskSlot);
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    private boolean markExistingSlotActive(TaskSlot<T> taskSlot) {
        if (taskSlot.markActive()) {
            // unregister a potential timeout
            LOG.info("Activate slot {}.", taskSlot.getAllocationId());

            timerService.unregisterTimeout(taskSlot.getAllocationId());

            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markSlotInactive(AllocationID allocationId, Duration slotTimeout)
            throws SlotNotFoundException {
        checkStarted();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            if (taskSlot.markInactive()) {
                // register a timeout to free the slot
                timerService.registerTimeout(
                        allocationId, slotTimeout.toMillis(), TimeUnit.MILLISECONDS);

                return true;
            } else {
                return false;
            }
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    @Override
    public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
        checkStarted();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return freeSlotInternal(taskSlot, cause).isDone() ? taskSlot.getIndex() : -1;
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    /**
     * 释放 Slot 的内部实现。
     *
     * <p>【学习型注释】Slot 释放流程（当 Slot 内没有运行中的 Task 时）：
     * <ol>
     *   <li>从 allocatedSlots 映射表移除</li>
     *   <li>取消超时定时器</li>
     *   <li>从 slotsPerJob 映射表移除</li>
     *   <li>从 taskSlots 映射表移除</li>
     *   <li>通过 budgetManager 释放资源预算</li>
     *   <li>异步关闭 TaskSlot（释放 MemoryManager 等资源）</li>
     * </ol>
     */
    private CompletableFuture<Void> freeSlotInternal(TaskSlot<T> taskSlot, Throwable cause) {
        AllocationID allocationId = taskSlot.getAllocationId();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Free slot {}.", taskSlot, cause);
        } else {
            LOG.info("Free slot {}.", taskSlot);
        }

        if (taskSlot.isEmpty()) {
            // remove the allocation id to task slot mapping
            allocatedSlots.remove(allocationId);

            // unregister a potential timeout
            timerService.unregisterTimeout(allocationId);

            JobID jobId = taskSlot.getJobId();
            Set<AllocationID> slots = slotsPerJob.get(jobId);

            if (slots == null) {
                throw new IllegalStateException(
                        "There are no more slots allocated for the job "
                                + jobId
                                + ". This indicates a programming bug.");
            }

            slots.remove(allocationId);

            if (slots.isEmpty()) {
                slotsPerJob.remove(jobId);
            }

            taskSlots.remove(taskSlot.getIndex());
            budgetManager.release(taskSlot.getResourceProfile());
        }
        return taskSlot.closeAsync(cause);
    }

    @Override
    public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
        checkStarted();

        return state == State.RUNNING && timerService.isValid(allocationId, ticket);
    }

    @Override
    public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
        TaskSlot<T> taskSlot = taskSlots.get(index);
        if (taskSlot != null) {
            return taskSlot.isAllocated(jobId, allocationId);
        } else {
            return false;
        }
    }

    @Override
    public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null && taskSlot.isAllocated(jobId, allocationId)) {
            return markExistingSlotActive(taskSlot);
        } else {
            return false;
        }
    }

    @Override
    public boolean isSlotFree(int index) {
        return !taskSlots.containsKey(index);
    }

    @Override
    public boolean hasAllocatedSlots(JobID jobId) {
        return getAllocatedSlots(jobId).hasNext();
    }

    @Override
    public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
        return new TaskSlotIterator(jobId, TaskSlotState.ALLOCATED);
    }

    @Override
    @Nullable
    public JobID getOwningJob(AllocationID allocationId) {
        final TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return taskSlot.getJobId();
        } else {
            return null;
        }
    }

    // ---------------------------------------------------------------------
    // Task methods
    // ---------------------------------------------------------------------

    /**
     * 将 Task 添加到指定的 Slot 中。
     *
     * <p>【学习型注释】Task 部署到 Slot 的前提条件：
     * <ul>
     *   <li>Slot 必须存在（通过 AllocationID 查找）</li>
     *   <li>Slot 必须处于 ACTIVE 状态（JobMaster 已确认分配）</li>
     *   <li>Task 的 JobID 和 AllocationID 必须与 Slot 匹配</li>
     * </ul>
     * 成功后，建立 ExecutionAttemptID → TaskSlotMapping 的映射关系。
     */
    @Override
    public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
        checkRunning();
        Preconditions.checkNotNull(task);

        TaskSlot<T> taskSlot = getTaskSlot(task.getAllocationId());

        if (taskSlot != null) {
            if (taskSlot.isActive(task.getJobID(), task.getAllocationId())) {
                if (taskSlot.add(task)) {
                    taskSlotMappings.put(
                            task.getExecutionId(), new TaskSlotMapping<>(task, taskSlot));

                    return true;
                } else {
                    return false;
                }
            } else {
                throw new SlotNotActiveException(task.getJobID(), task.getAllocationId());
            }
        } else {
            throw new SlotNotFoundException(task.getAllocationId());
        }
    }

    /**
     * 从 Slot 中移除指定的 Task。
     *
     * <p>【学习型注释】当 Task 移除后，如果 Slot 处于 RELEASING 状态且已为空，
     * 则触发 slotActions.freeSlot() 完成 Slot 的最终释放。
     */
    @Override
    public T removeTask(ExecutionAttemptID executionAttemptID) {
        checkStarted();

        TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.remove(executionAttemptID);

        if (taskSlotMapping != null) {
            T task = taskSlotMapping.getTask();
            TaskSlot<T> taskSlot = taskSlotMapping.getTaskSlot();

            taskSlot.remove(task.getExecutionId());

            if (taskSlot.isReleasing() && taskSlot.isEmpty()) {
                slotActions.freeSlot(taskSlot.getAllocationId());
            }

            return task;
        } else {
            return null;
        }
    }

    @Override
    public T getTask(ExecutionAttemptID executionAttemptID) {
        TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.get(executionAttemptID);

        if (taskSlotMapping != null) {
            return taskSlotMapping.getTask();
        } else {
            return null;
        }
    }

    @Override
    public Iterator<T> getTasks(JobID jobId) {
        return new PayloadIterator(jobId);
    }

    @Override
    public AllocationID getCurrentAllocation(int index) {
        TaskSlot<T> taskSlot = taskSlots.get(index);
        if (taskSlot == null) {
            return null;
        }
        return taskSlot.getAllocationId();
    }

    @Override
    public MemoryManager getTaskMemoryManager(AllocationID allocationID)
            throws SlotNotFoundException {
        TaskSlot<T> taskSlot = getTaskSlot(allocationID);
        if (taskSlot != null) {
            return taskSlot.getMemoryManager();
        } else {
            throw new SlotNotFoundException(allocationID);
        }
    }

    // ---------------------------------------------------------------------
    // TimeoutListener methods
    // ---------------------------------------------------------------------

    @Override
    public void notifyTimeout(AllocationID key, UUID ticket) {
        checkStarted();

        if (slotActions != null) {
            slotActions.timeoutSlot(key, ticket);
        }
    }

    // ---------------------------------------------------------------------
    // Internal methods
    // ---------------------------------------------------------------------

    @Nullable
    private TaskSlot<T> getTaskSlot(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);

        return allocatedSlots.get(allocationId);
    }

    private int nextDynamicSlotIndex() {
        return dynamicSlotIndex++;
    }

    private void checkRunning() {
        Preconditions.checkState(
                state == State.RUNNING,
                "The %s has to be running.",
                TaskSlotTableImpl.class.getSimpleName());
    }

    private void checkStarted() {
        Preconditions.checkState(
                state != State.CREATED,
                "The %s has to be started (not created).",
                TaskSlotTableImpl.class.getSimpleName());
    }

    // ---------------------------------------------------------------------
    // Static utility classes
    // ---------------------------------------------------------------------

    /** Mapping class between a {@link TaskSlotPayload} and a {@link TaskSlot}. */
    private static final class TaskSlotMapping<T extends TaskSlotPayload> {
        private final T task;
        private final TaskSlot<T> taskSlot;

        private TaskSlotMapping(T task, TaskSlot<T> taskSlot) {
            this.task = Preconditions.checkNotNull(task);
            this.taskSlot = Preconditions.checkNotNull(taskSlot);
        }

        public T getTask() {
            return task;
        }

        public TaskSlot<T> getTaskSlot() {
            return taskSlot;
        }
    }

    /**
     * Iterator over {@link TaskSlot} which fulfill a given state condition and belong to the given
     * job.
     */
    private final class TaskSlotIterator implements Iterator<TaskSlot<T>> {
        private final Iterator<AllocationID> allSlots;
        private final TaskSlotState state;

        private TaskSlot<T> currentSlot;

        private TaskSlotIterator(TaskSlotState state) {
            this(
                    slotsPerJob.values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet())
                            .iterator(),
                    state);
        }

        private TaskSlotIterator(JobID jobId, TaskSlotState state) {
            this(
                    slotsPerJob.get(jobId) == null
                            ? Collections.emptyIterator()
                            : slotsPerJob.get(jobId).iterator(),
                    state);
        }

        private TaskSlotIterator(Iterator<AllocationID> allocationIDIterator, TaskSlotState state) {
            this.allSlots = Preconditions.checkNotNull(allocationIDIterator);
            this.state = Preconditions.checkNotNull(state);
            this.currentSlot = null;
        }

        @Override
        public boolean hasNext() {
            while (currentSlot == null && allSlots.hasNext()) {
                AllocationID tempSlot = allSlots.next();

                TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

                if (taskSlot != null && taskSlot.getState() == state) {
                    currentSlot = taskSlot;
                }
            }

            return currentSlot != null;
        }

        @Override
        public TaskSlot<T> next() {
            if (currentSlot != null) {
                TaskSlot<T> result = currentSlot;

                currentSlot = null;

                return result;
            } else {
                while (true) {
                    AllocationID tempSlot;

                    try {
                        tempSlot = allSlots.next();
                    } catch (NoSuchElementException e) {
                        throw new NoSuchElementException("No more task slots.");
                    }

                    TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

                    if (taskSlot != null && taskSlot.getState() == state) {
                        return taskSlot;
                    }
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove task slots via this iterator.");
        }
    }

    /** Iterator over all {@link TaskSlotPayload} for a given job. */
    private final class PayloadIterator implements Iterator<T> {
        private final Iterator<TaskSlot<T>> taskSlotIterator;

        private Iterator<T> currentTasks;

        private PayloadIterator(JobID jobId) {
            this.taskSlotIterator = new TaskSlotIterator(jobId, TaskSlotState.ACTIVE);

            this.currentTasks = null;
        }

        @Override
        public boolean hasNext() {
            while ((currentTasks == null || !currentTasks.hasNext())
                    && taskSlotIterator.hasNext()) {
                TaskSlot<T> taskSlot = taskSlotIterator.next();

                currentTasks = taskSlot.getTasks();
            }

            return (currentTasks != null && currentTasks.hasNext());
        }

        @Override
        public T next() {
            while ((currentTasks == null || !currentTasks.hasNext())) {
                TaskSlot<T> taskSlot;

                try {
                    taskSlot = taskSlotIterator.next();
                } catch (NoSuchElementException e) {
                    throw new NoSuchElementException("No more tasks.");
                }

                currentTasks = taskSlot.getTasks();
            }

            return currentTasks.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove tasks via this iterator.");
        }
    }

    /**
     * TaskSlotTable 的生命周期状态。
     *
     * <p>状态转换：CREATED → RUNNING → CLOSING → CLOSED
     * <ul>
     *   <li>CREATED：初始状态，等待 start() 调用</li>
     *   <li>RUNNING：正常运行状态，可以分配/释放 Slot</li>
     *   <li>CLOSING：正在关闭，等待所有 Slot 释放完成</li>
     *   <li>CLOSED：已关闭，不再接受任何操作</li>
     * </ul>
     */
    private enum State {
        CREATED,
        RUNNING,
        CLOSING,
        CLOSED
    }
}
