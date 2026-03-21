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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.ThresholdMeter;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceAllocator;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceDeclaration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.FlinkExpectedException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An active implementation of {@link ResourceManager}.
 *
 * <p>This resource manager actively requests and releases resources from/to the external resource
 * management frameworks. With different {@link ResourceManagerDriver} provided, this resource
 * manager can work with various frameworks.
 *
 * <p>主动式资源管理器，继承自 {@link ResourceManager}，负责与外部资源管理框架（如 YARN、K8s、Mesos）交互。
 *
 * <p>与 {@link ResourceManager} 的区别：
 * <ul>
 *     <li><b>ResourceManager</b>：被动式，只管理已有的 Worker，不主动申请资源</li>
 *     <li><b>ActiveResourceManager</b>：主动式，根据 SlotManager 的资源声明主动向外部框架申请/释放 Worker</li>
 * </ul>
 *
 * <p>核心机制：
 * <ul>
 *     <li><b>声明式资源管理</b>：SlotManager 声明需要的资源数量，ActiveRM 负责达成目标状态</li>
 *     <li><b>Worker 生命周期</b>：
 *         <ol>
 *             <li>SlotManager 调用 {@link #declareResourceNeeded} 声明资源需求</li>
 *             <li>ActiveRM 调用 Driver 的 {@link ResourceManagerDriver#requestResource} 申请 Worker</li>
 *             <li>Driver 返回 WorkerType（如 YARN Container），Worker 进入 pending 状态</li>
 *             <li>Worker 启动后向 RM 注册，从 pending 变为 registered</li>
 *             <li>资源不再需要时，调用 Driver 的 {@link ResourceManagerDriver#releaseResource} 释放</li>
 *         </ol>
 *     </li>
 *     <li><b>故障恢复</b>：支持从前一次尝试恢复已有的 Worker，避免重复申请</li>
 *     <li><b>失败速率控制</b>：Worker 启动失败超过阈值时进入冷却期，避免频繁重试</li>
 * </ul>
 *
 * <p>与外部框架的交互通过 {@link ResourceManagerDriver} 抽象：
 * <ul>
 *     <li>YARN：{@code YarnResourceManagerDriver}</li>
 *     <li>Kubernetes：{@code KubernetesResourceManagerDriver}</li>
 *     <li>Standalone/Mesos 等</li>
 * </ul>
 *
 * <p>Worker 状态追踪：
 * <ul>
 *     <li>{@link #workerNodeMap}：所有已分配的 Worker（包括 pending 和 registered）</li>
 *     <li>{@link #currentAttemptUnregisteredWorkers}：当前尝试中已分配但未注册的 Worker</li>
 *     <li>{@link #previousAttemptUnregisteredWorkers}：前一次尝试恢复的但未注册的 Worker</li>
 *     <li>{@link #pendingWorkerCounter}：按资源规格统计的 pending Worker 数量</li>
 *     <li>{@link #totalWorkerCounter}：按资源规格统计的总 Worker 数量</li>
 * </ul>
 */
public class ActiveResourceManager<WorkerType extends ResourceIDRetrievable>
        extends ResourceManager<WorkerType> implements ResourceEventHandler<WorkerType> {

    /** Flink 配置对象，包含资源管理相关的配置参数 */
    protected final Configuration flinkConfig;

    /** Worker 启动失败后的重试间隔，避免频繁重试导致资源框架压力过大 */
    private final Duration startWorkerRetryInterval;

    /**
     * 资源管理驱动，封装了与外部资源框架的交互逻辑。
     * 不同框架有不同实现：YarnResourceManagerDriver、KubernetesResourceManagerDriver 等
     */
    private final ResourceManagerDriver<WorkerType> resourceManagerDriver;

    /**
     * 所有已分配的 Worker 映射表。
     * key: ResourceID（Worker 的唯一标识）
     * value: WorkerType（框架特定的 Worker 表示，如 YARN Container）
     */
    private final Map<ResourceID, WorkerType> workerNodeMap;

    /**
     * 按资源规格统计的 pending（已申请但未注册）Worker 数量。
     * 用于跟踪正在启动中的 Worker，避免重复申请。
     */
    private final WorkerCounter pendingWorkerCounter;

    /**
     * 按资源规格统计的总 Worker 数量（pending + registered）。
     * 用于与 SlotManager 声明的需求进行对比。
     */
    private final WorkerCounter totalWorkerCounter;

    /**
     * 每个 Worker 对应的资源规格。
     * 用于在释放 Worker 时正确更新计数器。
     */
    private final Map<ResourceID, WorkerResourceSpec> workerResourceSpecs;

    /**
     * 尚未分配（请求还在进行中）的 Worker Future 及其资源规格。
     * 用于在需要释放资源时取消正在进行的申请。
     */
    private final Map<CompletableFuture<WorkerType>, WorkerResourceSpec> unallocatedWorkerFutures;

    /**
     * 当前尝试中已申请但未注册的 Worker ID 集合。
     * Worker 注册后会从此集合移除，用于跟踪启动进度。
     */
    private final Set<ResourceID> currentAttemptUnregisteredWorkers;

    /**
     * 前一次尝试恢复的但未注册的 Worker ID 集合。
     * 用于 HA 故障恢复场景，区分新申请的和恢复的 Worker。
     */
    private final Set<ResourceID> previousAttemptUnregisteredWorkers;

    /**
     * Worker 启动失败速率计量器。
     * 超过阈值时触发冷却机制，暂停申请新 Worker。
     */
    private final ThresholdMeter startWorkerFailureRater;

    /**
     * Worker 注册超时时间。
     * Worker 分配后超过此时间仍未注册，会被停止并重新申请。
     */
    private final Duration workerRegistrationTimeout;

    /**
     * Worker 启动冷却标志。
     * 失败速率超过阈值时变为未完成状态，冷却期结束后完成。
     * 冷却期间不会申请新 Worker。
     */
    private CompletableFuture<Void> startWorkerCoolDown;

    /**
     * RM 就绪标志。
     * 当所有前一次尝试的 Worker 都恢复注册后完成，
     * 或者超过 previousWorkerRecoverTimeout 后强制完成。
     */
    private final CompletableFuture<Void> readyToServeFuture;

    /** 等待前一次尝试 Worker 恢复的超时时间 */
    private final Duration previousWorkerRecoverTimeout;

    /** SlotManager 声明的资源需求 */
    private Collection<ResourceDeclaration> resourceDeclarations;

    public ActiveResourceManager(
            ResourceManagerDriver<WorkerType> resourceManagerDriver,
            Configuration flinkConfig,
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ThresholdMeter startWorkerFailureRater,
            Duration retryInterval,
            Duration workerRegistrationTimeout,
            Duration previousWorkerRecoverTimeout,
            Executor ioExecutor) {
        super(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                delegationTokenManager,
                slotManager,
                clusterPartitionTrackerFactory,
                blocklistHandlerFactory,
                jobLeaderIdService,
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                Preconditions.checkNotNull(flinkConfig).get(RpcOptions.ASK_TIMEOUT_DURATION),
                ioExecutor);

        this.flinkConfig = flinkConfig;
        this.resourceManagerDriver = resourceManagerDriver;
        this.workerNodeMap = new HashMap<>();
        this.pendingWorkerCounter = new WorkerCounter();
        this.totalWorkerCounter = new WorkerCounter();
        this.workerResourceSpecs = new HashMap<>();
        this.unallocatedWorkerFutures = new HashMap<>();
        this.currentAttemptUnregisteredWorkers = new HashSet<>();
        this.previousAttemptUnregisteredWorkers = new HashSet<>();
        this.startWorkerFailureRater = checkNotNull(startWorkerFailureRater);
        this.startWorkerRetryInterval = retryInterval;
        this.workerRegistrationTimeout = workerRegistrationTimeout;
        this.startWorkerCoolDown = FutureUtils.completedVoidFuture();
        this.previousWorkerRecoverTimeout = previousWorkerRecoverTimeout;
        this.readyToServeFuture = new CompletableFuture<>();
        this.resourceDeclarations = new HashSet<>();
    }

    // ------------------------------------------------------------------------
    //  ResourceManager
    // ------------------------------------------------------------------------

    @Override
    protected void initialize() throws ResourceManagerException {
        try {
            resourceManagerDriver.initialize(
                    this,
                    new GatewayMainThreadExecutor(),
                    ioExecutor,
                    blocklistHandler::getAllBlockedNodeIds);
        } catch (Exception e) {
            throw new ResourceManagerException("Cannot initialize resource provider.", e);
        }
    }

    @Override
    protected void terminate() throws ResourceManagerException {
        try {
            resourceManagerDriver.terminate();
        } catch (Exception e) {
            throw new ResourceManagerException("Cannot terminate resource provider.", e);
        }
    }

    @Override
    protected void internalDeregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics)
            throws ResourceManagerException {
        try {
            resourceManagerDriver.deregisterApplication(finalStatus, optionalDiagnostics);
        } catch (Exception e) {
            throw new ResourceManagerException("Cannot deregister application.", e);
        }
    }

    @Override
    protected Optional<WorkerType> getWorkerNodeIfAcceptRegistration(ResourceID resourceID) {
        return Optional.ofNullable(workerNodeMap.get(resourceID));
    }

    @VisibleForTesting
    public void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations) {
        this.resourceDeclarations = Collections.unmodifiableCollection(resourceDeclarations);
        log.debug("Update resource declarations to {}.", resourceDeclarations);

        checkResourceDeclarations();
    }

    @Override
    protected void onWorkerRegistered(WorkerType worker, WorkerResourceSpec workerResourceSpec) {
        final ResourceID resourceId = worker.getResourceID();
        log.info("Worker {} is registered.", resourceId.getStringWithMetadata());

        tryRemovePreviousPendingRecoveryTaskManager(resourceId);

        if (!workerResourceSpecs.containsKey(worker.getResourceID())) {
            workerResourceSpecs.put(worker.getResourceID(), workerResourceSpec);
            totalWorkerCounter.increaseAndGet(workerResourceSpec);
            log.info(
                    "Recovered worker {} with resource spec {} registered",
                    resourceId.getStringWithMetadata(),
                    workerResourceSpec);
        }

        if (currentAttemptUnregisteredWorkers.remove(resourceId)) {
            final int count = pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
            log.info(
                    "Worker {} with resource spec {} was requested in current attempt."
                            + " Current pending count after registering: {}.",
                    resourceId.getStringWithMetadata(),
                    workerResourceSpec,
                    count);
        }
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();
        resourceManagerMetricGroup.meter(
                MetricNames.START_WORKER_FAILURE_RATE, startWorkerFailureRater);
        resourceManagerMetricGroup.gauge(
                MetricNames.NUM_PENDING_TASK_MANAGERS, pendingWorkerCounter::getTotalNum);
    }

    // ------------------------------------------------------------------------
    //  ResourceEventListener
    // ------------------------------------------------------------------------

    @Override
    public void onPreviousAttemptWorkersRecovered(Collection<WorkerType> recoveredWorkers) {
        getMainThreadExecutor().assertRunningInMainThread();
        log.info("Recovered {} workers from previous attempt.", recoveredWorkers.size());
        for (WorkerType worker : recoveredWorkers) {
            final ResourceID resourceId = worker.getResourceID();
            workerNodeMap.put(resourceId, worker);
            previousAttemptUnregisteredWorkers.add(resourceId);
            scheduleWorkerRegistrationTimeoutCheck(resourceId);
            log.info(
                    "Worker {} recovered from previous attempt.",
                    resourceId.getStringWithMetadata());
        }
        if (recoveredWorkers.size() > 0 && !previousWorkerRecoverTimeout.isZero()) {
            scheduleRunAsync(
                    () -> {
                        readyToServeFuture.complete(null);
                        log.info(
                                "Timeout to wait recovery taskmanagers, recovery future is completed.");
                    },
                    previousWorkerRecoverTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        } else {
            readyToServeFuture.complete(null);
        }
    }

    @Override
    public void onWorkerTerminated(ResourceID resourceId, String diagnostics) {
        if (currentAttemptUnregisteredWorkers.contains(resourceId)) {
            recordWorkerFailureAndPauseWorkerCreationIfNeeded();
        }

        if (clearStateForWorker(resourceId)) {
            log.info(
                    "Worker {} is terminated. Diagnostics: {}",
                    resourceId.getStringWithMetadata(),
                    diagnostics);
            checkResourceDeclarations();
        }
        closeTaskManagerConnection(resourceId, new Exception(diagnostics));
    }

    @Override
    public void onError(Throwable exception) {
        onFatalError(exception);
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    /**
     * 检查并协调资源声明与实际 Worker 数量。
     *
     * <p>这是 ActiveResourceManager 的核心方法，负责达成 SlotManager 声明的目标状态：
     * <ul>
     *     <li><b>资源过剩</b>（当前 Worker 数 > 声明数）：按优先级释放多余 Worker
     *         <ol>
     *             <li>释放 SlotManager 标记为 unwanted 的 Worker</li>
     *             <li>取消尚未分配的 Worker 申请</li>
     *             <li>释放已分配但未注册的 Worker</li>
     *             <li>释放已注册的 Worker</li>
     *         </ol>
     *     </li>
     *     <li><b>资源不足</b>（当前 Worker 数 < 声明数）：申请新 Worker
     *         <ul>
     *             <li>如果在冷却期，等待冷却结束后重试</li>
     *             <li>否则立即申请缺少的 Worker 数量</li>
     *         </ul>
     *     </li>
     *     <li><b>资源匹配</b>：无需操作</li>
     * </ul>
     */
    private void checkResourceDeclarations() {
        validateRunsInMainThread();

        for (ResourceDeclaration resourceDeclaration : resourceDeclarations) {
            WorkerResourceSpec workerResourceSpec = resourceDeclaration.getSpec();
            int declaredWorkerNumber = resourceDeclaration.getNumNeeded();

            // 计算需要释放或申请的 Worker 数量
            // 正数表示需要释放，负数表示需要申请
            final int releaseOrRequestWorkerNumber =
                    totalWorkerCounter.getNum(workerResourceSpec) - declaredWorkerNumber;

            if (releaseOrRequestWorkerNumber > 0) {
                // 资源过剩，需要释放 Worker
                log.info(
                        "need release {} workers, current worker number {}, declared worker number {}",
                        releaseOrRequestWorkerNumber,
                        totalWorkerCounter.getNum(workerResourceSpec),
                        declaredWorkerNumber);

                // 按优先级释放：unwanted > unallocated > starting > registered
                int remainingReleasingWorkerNumber =
                        releaseUnWantedResources(
                                resourceDeclaration.getUnwantedWorkers(),
                                releaseOrRequestWorkerNumber);

                if (remainingReleasingWorkerNumber > 0) {
                    // 取消尚未分配的 Worker 申请
                    remainingReleasingWorkerNumber =
                            releaseUnallocatedWorkers(
                                    workerResourceSpec, remainingReleasingWorkerNumber);
                }

                if (remainingReleasingWorkerNumber > 0) {
                    // 释放已分配但未注册的 Worker
                    remainingReleasingWorkerNumber =
                            releaseAllocatedWorkers(
                                    currentAttemptUnregisteredWorkers,
                                    workerResourceSpec,
                                    remainingReleasingWorkerNumber);
                }

                if (remainingReleasingWorkerNumber > 0) {
                    // 释放已注册的 Worker
                    remainingReleasingWorkerNumber =
                            releaseAllocatedWorkers(
                                    workerNodeMap.keySet(),
                                    workerResourceSpec,
                                    remainingReleasingWorkerNumber);
                }

                checkState(
                        remainingReleasingWorkerNumber == 0,
                        "there are no more workers to release");
            } else if (releaseOrRequestWorkerNumber < 0) {
                // 资源不足，需要申请新 Worker
                // 检查是否在冷却期，避免失败后频繁重试
                if (startWorkerCoolDown.isDone()) {
                    int requestWorkerNumber = -releaseOrRequestWorkerNumber;
                    log.info(
                            "need request {} new workers, current worker number {}, declared worker number {}",
                            requestWorkerNumber,
                            totalWorkerCounter.getNum(workerResourceSpec),
                            declaredWorkerNumber);
                    // 逐个申请 Worker
                    for (int i = 0; i < requestWorkerNumber; i++) {
                        requestNewWorker(workerResourceSpec);
                    }
                } else {
                    // 在冷却期，等待冷却结束后重新检查
                    startWorkerCoolDown.thenRun(this::checkResourceDeclarations);
                }
            } else {
                // 资源匹配，无需操作
                log.debug(
                        "current worker number {} meets the declared worker {}",
                        totalWorkerCounter.getNum(workerResourceSpec),
                        declaredWorkerNumber);
            }
        }
    }

    private int releaseUnWantedResources(
            Collection<InstanceID> unwantedWorkers, int needReleaseWorkerNumber) {

        Exception cause =
                new FlinkExpectedException(
                        "slot manager has determined that the resource is no longer needed");
        for (InstanceID unwantedWorker : unwantedWorkers) {
            if (needReleaseWorkerNumber <= 0) {
                break;
            }
            if (releaseResource(unwantedWorker, cause)) {
                needReleaseWorkerNumber--;
            }
        }
        return needReleaseWorkerNumber;
    }

    private int releaseUnallocatedWorkers(
            WorkerResourceSpec workerResourceSpec, int needReleaseWorkerNumber) {
        Set<CompletableFuture<WorkerType>> unallocatedWorkerFuturesShouldRelease =
                unallocatedWorkerFutures.entrySet().stream()
                        .filter(e -> e.getValue().equals(workerResourceSpec))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
        for (CompletableFuture<WorkerType> requestFuture : unallocatedWorkerFuturesShouldRelease) {
            if (needReleaseWorkerNumber <= 0) {
                break;
            }
            if (requestFuture.cancel(true)) {
                needReleaseWorkerNumber--;
            }
        }
        return needReleaseWorkerNumber;
    }

    private int releaseAllocatedWorkers(
            Collection<ResourceID> candidateWorkers,
            WorkerResourceSpec workerResourceSpec,
            int needReleaseWorkerNumber) {
        List<ResourceID> workerCanRelease =
                candidateWorkers.stream()
                        .filter(r -> workerResourceSpec.equals(workerResourceSpecs.get(r)))
                        .collect(Collectors.toList());

        Exception cause = new FlinkExpectedException("resource is no longer needed");
        for (ResourceID resourceID : workerCanRelease) {
            if (needReleaseWorkerNumber <= 0) {
                break;
            }

            if (releaseResource(resourceID, cause)) {
                needReleaseWorkerNumber--;
            } else {
                log.warn("Resource {} could not release.", resourceID);
            }
        }

        return needReleaseWorkerNumber;
    }

    private boolean releaseResource(InstanceID instanceId, Exception cause) {
        WorkerType worker = getWorkerByInstanceId(instanceId);
        if (worker != null) {
            return releaseResource(worker.getResourceID(), cause);
        } else {
            log.debug("Instance {} not found in ResourceManager.", instanceId);
            return false;
        }
    }

    private boolean releaseResource(ResourceID resourceID, Exception cause) {
        if (workerNodeMap.containsKey(resourceID)) {
            internalStopWorker(resourceID);
            closeTaskManagerConnection(resourceID, cause);
            return true;
        }
        return false;
    }

    /**
     * Allocates a resource using the worker resource specification.
     *
     * <p>向外部资源框架申请新的 Worker。
     *
     * <p>申请流程：
     * <ol>
     *     <li>将 WorkerResourceSpec 转换为 TaskExecutorProcessSpec（包含 JVM 参数等）</li>
     *     <li>更新计数器（pendingWorkerCounter、totalWorkerCounter）</li>
     *     <li>调用 Driver 的 requestResource 异步申请资源</li>
     *     <li>处理异步结果：
     *         <ul>
     *             <li>成功：记录 Worker 信息，启动注册超时检查</li>
     *             <li>失败/取消：回滚计数器，记录失败并可能触发冷却</li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param workerResourceSpec workerResourceSpec specifies the size of the to be allocated
     *     resource
     */
    @VisibleForTesting
    public void requestNewWorker(WorkerResourceSpec workerResourceSpec) {
        // 将资源规格转换为进程规格（包含内存、CPU 等 JVM 配置）
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                        flinkConfig, workerResourceSpec);
        // 更新 pending 计数
        final int pendingCount = pendingWorkerCounter.increaseAndGet(workerResourceSpec);
        totalWorkerCounter.increaseAndGet(workerResourceSpec);

        log.info(
                "Requesting new worker with resource spec {}, current pending count: {}.",
                workerResourceSpec,
                pendingCount);

        // 异步申请资源
        final CompletableFuture<WorkerType> requestResourceFuture =
                resourceManagerDriver.requestResource(taskExecutorProcessSpec);
        unallocatedWorkerFutures.put(requestResourceFuture, workerResourceSpec);

        // 处理异步结果
        FutureUtils.assertNoException(
                requestResourceFuture.handle(
                        (worker, exception) -> {
                            // 从 unallocated 集合移除
                            unallocatedWorkerFutures.remove(requestResourceFuture);

                            if (exception != null) {
                                // 申请失败，回滚计数器
                                final int count =
                                        pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
                                totalWorkerCounter.decreaseAndGet(workerResourceSpec);
                                if (exception instanceof CancellationException) {
                                    // 被主动取消（资源不再需要）
                                    log.info(
                                            "Requesting worker with resource spec {} canceled, current pending count: {}",
                                            workerResourceSpec,
                                            count);
                                } else {
                                    // 申请失败
                                    log.warn(
                                            "Failed requesting worker with resource spec {}, current pending count: {}",
                                            workerResourceSpec,
                                            count,
                                            exception);
                                    // 记录失败，可能触发冷却
                                    recordWorkerFailureAndPauseWorkerCreationIfNeeded();
                                    // 重新检查资源声明，可能需要重试
                                    checkResourceDeclarations();
                                }
                            } else {
                                // 申请成功，记录 Worker 信息
                                final ResourceID resourceId = worker.getResourceID();
                                workerNodeMap.put(resourceId, worker);
                                workerResourceSpecs.put(resourceId, workerResourceSpec);
                                currentAttemptUnregisteredWorkers.add(resourceId);
                                // 启动注册超时检查
                                scheduleWorkerRegistrationTimeoutCheck(resourceId);
                                log.info(
                                        "Requested worker {} with resource spec {}.",
                                        resourceId.getStringWithMetadata(),
                                        workerResourceSpec);
                            }
                            return null;
                        }));
    }

    private void scheduleWorkerRegistrationTimeoutCheck(final ResourceID resourceId) {
        scheduleRunAsync(
                () -> {
                    if (currentAttemptUnregisteredWorkers.contains(resourceId)
                            || previousAttemptUnregisteredWorkers.contains(resourceId)) {
                        log.warn(
                                "Worker {} did not register in {}, will stop it and request a new one if needed.",
                                resourceId,
                                workerRegistrationTimeout);
                        internalStopWorker(resourceId);
                        checkResourceDeclarations();
                    }
                },
                workerRegistrationTimeout);
    }

    private void internalStopWorker(final ResourceID resourceId) {
        log.info("Stopping worker {}.", resourceId.getStringWithMetadata());

        final WorkerType worker = workerNodeMap.get(resourceId);
        if (worker != null) {
            resourceManagerDriver.releaseResource(worker);
        }

        clearStateForWorker(resourceId);
    }

    /**
     * Clear states for a terminated worker.
     *
     * @param resourceId Identifier of the worker
     * @return True if the worker is known and states are cleared; false if the worker is unknown
     *     (duplicate call to already cleared worker)
     */
    private boolean clearStateForWorker(ResourceID resourceId) {
        WorkerType worker = workerNodeMap.remove(resourceId);
        if (worker == null) {
            log.debug("Ignore unrecognized worker {}.", resourceId.getStringWithMetadata());
            return false;
        }

        WorkerResourceSpec workerResourceSpec = workerResourceSpecs.remove(resourceId);
        tryRemovePreviousPendingRecoveryTaskManager(resourceId);
        if (workerResourceSpec != null) {
            totalWorkerCounter.decreaseAndGet(workerResourceSpec);
            if (currentAttemptUnregisteredWorkers.remove(resourceId)) {
                final int count = pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
                log.info(
                        "Worker {} with resource spec {} was requested in current attempt and has not registered."
                                + " Current pending count after removing: {}.",
                        resourceId.getStringWithMetadata(),
                        workerResourceSpec,
                        count);
            }
        }
        return true;
    }

    private void recordWorkerFailureAndPauseWorkerCreationIfNeeded() {
        if (recordStartWorkerFailure()) {
            // if exceed failure rate try to slow down
            tryResetWorkerCreationCoolDown();
        }
    }

    /**
     * Record failure number of starting worker in ResourceManagers. Return whether maximum failure
     * rate is reached.
     *
     * @return whether max failure rate is reached
     */
    private boolean recordStartWorkerFailure() {
        startWorkerFailureRater.markEvent();

        try {
            startWorkerFailureRater.checkAgainstThreshold();
        } catch (ThresholdMeter.ThresholdExceedException e) {
            log.warn("Reaching max start worker failure rate: {}", e.getMessage());
            return true;
        }

        return false;
    }

    private void tryResetWorkerCreationCoolDown() {
        if (startWorkerCoolDown.isDone()) {
            log.info("Will not retry creating worker in {}.", startWorkerRetryInterval);
            startWorkerCoolDown = new CompletableFuture<>();
            scheduleRunAsync(() -> startWorkerCoolDown.complete(null), startWorkerRetryInterval);
        }
    }

    @Override
    public CompletableFuture<Void> getReadyToServeFuture() {
        return readyToServeFuture;
    }

    @Override
    protected ResourceAllocator getResourceAllocator() {
        return new ResourceAllocatorImpl();
    }

    private void tryRemovePreviousPendingRecoveryTaskManager(ResourceID resourceID) {
        long sizeBeforeRemove = previousAttemptUnregisteredWorkers.size();
        if (previousAttemptUnregisteredWorkers.remove(resourceID)) {
            log.info(
                    "Pending recovery taskmanagers {} -> {}.{}",
                    sizeBeforeRemove,
                    previousAttemptUnregisteredWorkers.size(),
                    previousAttemptUnregisteredWorkers.size() == 0
                            ? " Resource manager is ready to serve."
                            : "");
        }
        if (previousAttemptUnregisteredWorkers.size() == 0) {
            readyToServeFuture.complete(null);
        }
    }

    /** Always execute on the current main thread executor. */
    private class GatewayMainThreadExecutor implements ScheduledExecutor {

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return getMainThreadExecutor().schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return getMainThreadExecutor().schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            return getMainThreadExecutor().scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return getMainThreadExecutor()
                    .scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public void execute(Runnable command) {
            getMainThreadExecutor().execute(command);
        }
    }

    private class ResourceAllocatorImpl implements ResourceAllocator {

        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public void cleaningUpDisconnectedResource(ResourceID resourceID) {
            validateRunsInMainThread();
            internalStopWorker(resourceID);
        }

        @Override
        public void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations) {
            validateRunsInMainThread();
            ActiveResourceManager.this.declareResourceNeeded(resourceDeclarations);
        }
    }

    // ------------------------------------------------------------------------
    //  Testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    <T> CompletableFuture<T> runInMainThread(Callable<T> callable, Duration timeout) {
        return callAsync(callable, timeout);
    }
}
