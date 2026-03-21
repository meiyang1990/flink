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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 *
 * <p>{@code DefaultLeaderElectionService} handles a single {@link LeaderContender}.
 *
 * <p>【学习笔记】DefaultLeaderElectionService 是 Flink Leader 选举的默认实现。
 *
 * <h3>一、设计模式</h3>
 * <p>采用<b>策略模式</b>，通过 LeaderElectionDriver 适配不同的 HA 后端：
 * <ul>
 *   <li>{@code ZooKeeperLeaderElectionDriver}：基于 ZooKeeper 的选举</li>
 *   <li>{@code KubernetesLeaderElectionDriver}：基于 K8s ConfigMap 的选举</li>
 * </ul>
 *
 * <h3>二、核心状态</h3>
 * <ul>
 *   <li><b>issuedLeaderSessionID</b>：当前颁发的 Leader Session ID，null 表示非 Leader</li>
 *   <li><b>confirmedLeaderInformation</b>：已确认的 Leader 信息注册表</li>
 *   <li><b>leaderContenderRegistry</b>：组件 ID → LeaderContender 的映射</li>
 * </ul>
 *
 * <h3>三、生命周期</h3>
 * <ol>
 *   <li><b>createLeaderElection()</b>：为组件创建选举实例</li>
 *   <li><b>register()</b>：注册竞选者，首次注册时创建 Driver 并连接 HA 后端</li>
 *   <li><b>onGrantLeadership()</b>：HA 后端通知获得领导权</li>
 *   <li><b>confirmLeadershipAsync()</b>：确认领导权并发布 Leader 信息</li>
 *   <li><b>onRevokeLeadership()</b>：HA 后端通知失去领导权</li>
 *   <li><b>remove()</b>：取消注册，最后一个竞选者移除时关闭 Driver</li>
 * </ol>
 *
 * <h3>四、线程模型</h3>
 * <p>使用单线程 ExecutorService（leadershipOperationExecutor）处理所有选举事件，
 * 确保事件顺序执行，避免并发问题。
 *
 * <h3>五、与 LeaderContender 的交互</h3>
 * <ul>
 *   <li>{@code grantLeadership()}：通知竞选者获得领导权</li>
 *   <li>{@code revokeLeadership()}：通知竞选者失去领导权</li>
 *   <li>{@code handleError()}：通知竞选者发生错误</li>
 * </ul>
 */
public class DefaultLeaderElectionService extends DefaultLeaderElection.ParentService
        implements LeaderElectionService, LeaderElectionDriver.Listener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    // 日志事件名称常量
    private static final String LEADER_ACQUISITION_EVENT_LOG_NAME = "Leader Acquisition";
    private static final String LEADER_REVOCATION_EVENT_LOG_NAME = "Leader Revocation";

    // 同步锁，保护内部状态的一致性
    private final Object lock = new Object();

    // Leader 选举驱动工厂，用于创建具体的 HA 后端驱动（ZooKeeper/Kubernetes）
    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    /**
     * 组件 ID → LeaderContender 的映射表。
     * 支持多个组件共用同一个选举服务（如 Dispatcher、ResourceManager）。
     */
    @GuardedBy("lock")
    private final Map<String, LeaderContender> leaderContenderRegistry = new HashMap<>();

    /**
     * Saves the session ID which was issued by the {@link LeaderElectionDriver} if and only if the
     * leadership is acquired by this service. {@code issuedLeaderSessionID} being {@code null}
     * indicates that this service isn't the leader right now (i.e. {@link
     * #onGrantLeadership(UUID)}) wasn't called, yet (independently of what {@code
     * leaderElectionDriver#hasLeadership()} returns).
     *
     * <p>当前颁发的 Leader Session ID。null 表示当前不是 Leader。
     * 这是判断是否持有领导权的核心标志。
     */
    @GuardedBy("lock")
    @Nullable
    private UUID issuedLeaderSessionID;

    /**
     * Saves the {@link LeaderInformation} for the registered {@link LeaderContender}s. There's no
     * semantic difference between an entry with an empty {@code LeaderInformation} and no entry
     * being present at all here. Both mean that no confirmed {@code LeaderInformation} is available
     * for the corresponding {@code componentId}.
     *
     * <p>已确认的 Leader 信息注册表，存储每个组件确认后的 Leader 地址和 Session ID。
     */
    @GuardedBy("lock")
    private LeaderInformationRegister confirmedLeaderInformation;

    // 服务运行状态
    @GuardedBy("lock")
    private boolean running;

    /**
     * The driver's lifecycle is bound to the {@link #leaderContenderRegistry}: {@code
     * leaderElectionDriver} is {@code null} if no contender is registered: A new driver is created
     * as soon as the first contender is added to the empty {@code leaderContenderRegistry}. Only
     * then, a connection to the {@code DefaultLeaderElectionService} backend is established. The
     * service resets and closes the driver with the removal of the last contender.
     *
     * <p>Leader 选举驱动器，负责与 HA 后端（ZooKeeper/Kubernetes）交互。
     * 生命周期与 leaderContenderRegistry 绑定：第一个竞选者注册时创建，最后一个竞选者移除时关闭。
     */
    @GuardedBy("lock")
    private LeaderElectionDriver leaderElectionDriver;

    /**
     * This {@link ExecutorService} is used for running the leader event handling logic. Production
     * code should rely on a single-threaded executor to ensure the sequential execution of the
     * events.
     *
     * <p>The executor is guarded by this instance's {@link #running} state.
     *
     * <p>领导权事件处理线程池，使用单线程确保事件顺序执行。
     * 所有选举相关的回调（获得/失去领导权）都在此线程中处理。
     */
    private final ExecutorService leadershipOperationExecutor;

    // 后备错误处理器，当没有注册的竞选者时使用
    private final FatalErrorHandler fallbackErrorHandler;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this(
                leaderElectionDriverFactory,
                t ->
                        LOG.debug(
                                "Ignoring error notification since there's no contender registered."));
    }

    @VisibleForTesting
    public DefaultLeaderElectionService(
            LeaderElectionDriverFactory leaderElectionDriverFactory,
            FatalErrorHandler fallbackErrorHandler) {
        this(
                leaderElectionDriverFactory,
                fallbackErrorHandler,
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "DefaultLeaderElectionService-leadershipOperationExecutor")));
    }

    @VisibleForTesting
    DefaultLeaderElectionService(
            LeaderElectionDriverFactory leaderElectionDriverFactory,
            FatalErrorHandler fallbackErrorHandler,
            ExecutorService leadershipOperationExecutor) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        this.fallbackErrorHandler = checkNotNull(fallbackErrorHandler);

        this.issuedLeaderSessionID = null;

        this.leaderElectionDriver = null;

        this.confirmedLeaderInformation = LeaderInformationRegister.empty();

        this.leadershipOperationExecutor = Preconditions.checkNotNull(leadershipOperationExecutor);

        this.running = true;
    }

    @Override
    public LeaderElection createLeaderElection(String componentId) {
        synchronized (lock) {
            Preconditions.checkState(
                    !leadershipOperationExecutor.isShutdown(),
                    "The service was already closed and cannot be reused.");
            Preconditions.checkState(
                    !leaderContenderRegistry.containsKey(componentId),
                    "There shouldn't be any contender registered under the passed component '%s'.",
                    componentId);
            return new DefaultLeaderElection(this, componentId);
        }
    }

    @GuardedBy("lock")
    private void createLeaderElectionDriver() throws Exception {
        Preconditions.checkState(
                leaderContenderRegistry.isEmpty(),
                "No LeaderContender should have been registered, yet.");
        Preconditions.checkState(
                leaderElectionDriver == null,
                "This DefaultLeaderElectionService cannot be reused. Calling startLeaderElectionBackend can only be called once to establish the connection to the HA backend.");

        leaderElectionDriver = leaderElectionDriverFactory.create(this);

        LOG.info(
                "A connection to the HA backend was established through LeaderElectionDriver {}.",
                leaderElectionDriver);
    }

    @Override
    protected void register(String componentId, LeaderContender contender) throws Exception {
        checkNotNull(componentId, "componentId must not be null.");
        checkNotNull(contender, "Contender must not be null.");

        synchronized (lock) {
            Preconditions.checkState(
                    running,
                    "The DefaultLeaderElectionService should have established a connection to the backend before it's started.");

            if (leaderElectionDriver == null) {
                createLeaderElectionDriver();
            }

            Preconditions.checkState(
                    leaderContenderRegistry.put(componentId, contender) == null,
                    "There shouldn't be any contender registered under the passed component '%s'.",
                    componentId);

            LOG.info(
                    "LeaderContender has been registered under component '{}' for {}.",
                    componentId,
                    leaderElectionDriver);

            if (issuedLeaderSessionID != null) {
                // notifying the LeaderContender shouldn't happen in the contender's main thread
                runInLeaderEventThread(
                        LEADER_ACQUISITION_EVENT_LOG_NAME,
                        () ->
                                notifyLeaderContenderOfLeadership(
                                        componentId, issuedLeaderSessionID));
            }
        }
    }

    @Override
    protected final void remove(String componentId) throws Exception {
        AutoCloseable driverToClose = null;
        synchronized (lock) {
            if (!leaderContenderRegistry.containsKey(componentId)) {
                LOG.debug(
                        "There is no contender registered under component '{}' anymore. No action necessary.",
                        componentId);
                return;
            }
            Preconditions.checkState(
                    leaderElectionDriver != null,
                    "The LeaderElectionDriver should be instantiated.");

            LOG.info(
                    "Deregistering contender with component '{}' from the DefaultLeaderElectionService.",
                    componentId);

            final LeaderContender leaderContender = leaderContenderRegistry.remove(componentId);
            Preconditions.checkNotNull(
                    leaderContender,
                    "There should be a LeaderContender registered under the given component '%s'.",
                    componentId);
            if (issuedLeaderSessionID != null) {
                notifyLeaderContenderOfLeadershipLoss(componentId, leaderContender);
                LOG.debug(
                        "The contender associated with component '{}' is deregistered while the service has the leadership acquired. The revoke event is forwarded to the LeaderContender.",
                        componentId);

                if (leaderElectionDriver.hasLeadership()) {
                    leaderElectionDriver.deleteLeaderInformation(componentId);
                    LOG.debug(
                            "Leader information is cleaned up while deregistering the contender for component '{}' from the service.",
                            componentId);
                }
            } else {
                Preconditions.checkState(
                        confirmedLeaderInformation.hasNoLeaderInformation(),
                        "The confirmed leader information should have been cleared during leadership revocation.");

                LOG.debug(
                        "Contender associated with component '{}' is deregistered while the service doesn't have the leadership acquired. No cleanup necessary.",
                        componentId);
            }

            if (leaderContenderRegistry.isEmpty()) {
                driverToClose = deregisterDriver();
            }
        }

        if (driverToClose != null) {
            driverToClose.close();
        }
    }

    /**
     * Returns the driver as an {@link AutoCloseable} for the sake of closing the driver outside of
     * the lock.
     */
    @GuardedBy("lock")
    private AutoCloseable deregisterDriver() {
        Preconditions.checkState(
                leaderContenderRegistry.isEmpty(),
                "No contender should be registered when deregistering the driver.");
        Preconditions.checkState(
                leaderElectionDriver != null,
                "There should be a driver instantiated that's ready to be closed.");

        issuedLeaderSessionID = null;
        final AutoCloseable driverToClose = leaderElectionDriver;
        leaderElectionDriver = null;

        return driverToClose;
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderContenderRegistry.isEmpty(),
                    "The DefaultLeaderElectionService should have been stopped before closing the instance.");
            Preconditions.checkState(
                    leaderElectionDriver == null, "The driver should have been closed.");

            if (running) {
                running = false;
            } else {
                LOG.debug("The HA backend connection isn't established. No actions taken.");
                return;
            }
        }

        // interrupt any outstanding events
        final List<Runnable> outstandingEventHandlingCalls =
                leadershipOperationExecutor.shutdownNow();
        if (!outstandingEventHandlingCalls.isEmpty()) {
            LOG.debug(
                    "The DefaultLeaderElectionService was closed with {} event(s) still not being processed. No further action necessary.",
                    outstandingEventHandlingCalls.size());
        }
    }

    @Override
    protected CompletableFuture<Void> confirmLeadershipAsync(
            String componentId, UUID leaderSessionID, String leaderAddress) {
        Preconditions.checkArgument(leaderContenderRegistry.containsKey(componentId));
        LOG.debug(
                "The leader session for component '{}' is confirmed with session ID {} and address {}.",
                componentId,
                leaderSessionID,
                leaderAddress);

        checkNotNull(leaderSessionID);

        return CompletableFuture.runAsync(
                () -> {
                    synchronized (lock) {
                        if (hasLeadershipInternal(componentId, leaderSessionID)) {
                            Preconditions.checkState(
                                    leaderElectionDriver != null,
                                    "The leadership check should only return true if a driver is instantiated.");
                            Preconditions.checkState(
                                    !confirmedLeaderInformation.hasLeaderInformation(componentId),
                                    "No confirmation should have happened, yet.");

                            final LeaderInformation newConfirmedLeaderInformation =
                                    LeaderInformation.known(leaderSessionID, leaderAddress);
                            confirmedLeaderInformation =
                                    LeaderInformationRegister.merge(
                                            confirmedLeaderInformation,
                                            componentId,
                                            newConfirmedLeaderInformation);
                            leaderElectionDriver.publishLeaderInformation(
                                    componentId, newConfirmedLeaderInformation);
                        } else {
                            if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                                LOG.debug(
                                        "Received an old confirmation call of leader session ID {} for component '{}' (current issued session ID is {}).",
                                        leaderSessionID,
                                        componentId,
                                        issuedLeaderSessionID);
                            } else {
                                LOG.warn(
                                        "The leader session ID {} for component '{}' was confirmed even though the corresponding "
                                                + "service was not elected as the leader or has been stopped already.",
                                        componentId,
                                        leaderSessionID);
                            }
                        }
                    }
                },
                leadershipOperationExecutor);
    }

    @Override
    protected CompletableFuture<Boolean> hasLeadershipAsync(
            String componentId, UUID leaderSessionId) {
        return CompletableFuture.supplyAsync(
                () -> {
                    synchronized (lock) {
                        return hasLeadershipInternal(componentId, leaderSessionId);
                    }
                },
                leadershipOperationExecutor);
    }

    @GuardedBy("lock")
    private boolean hasLeadershipInternal(String componentId, UUID leaderSessionId) {
        if (leaderElectionDriver != null) {
            if (leaderContenderRegistry.containsKey(componentId)) {
                return leaderElectionDriver.hasLeadership()
                        && leaderSessionId.equals(issuedLeaderSessionID);
            } else {
                LOG.debug(
                        "hasLeadership is called for component '{}' while there is no contender registered under that ID in the service, returning false.",
                        componentId);
                return false;
            }
        } else {
            LOG.debug("hasLeadership is called after the service is closed, returning false.");
            return false;
        }
    }

    /**
     * Returns the current leader session ID for the given {@code componentId} or {@code null}, if
     * the session wasn't confirmed.
     */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID(String componentId) {
        synchronized (lock) {
            return leaderContenderRegistry.containsKey(componentId)
                    ? confirmedLeaderInformation
                            .forComponentIdOrEmpty(componentId)
                            .getLeaderSessionID()
                    : null;
        }
    }

    @GuardedBy("lock")
    private void onGrantLeadershipInternal(UUID newLeaderSessionId) {
        Preconditions.checkNotNull(newLeaderSessionId);

        Preconditions.checkState(
                issuedLeaderSessionID == null,
                "The leadership should have been granted while not having the leadership acquired.");

        issuedLeaderSessionID = newLeaderSessionId;

        leaderContenderRegistry
                .keySet()
                .forEach(
                        componentId ->
                                notifyLeaderContenderOfLeadership(
                                        componentId, issuedLeaderSessionID));
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadership(String componentId, UUID sessionID) {
        if (!leaderContenderRegistry.containsKey(componentId)) {
            LOG.debug(
                    "The grant leadership notification for session ID {} is not forwarded because the DefaultLeaderElectionService ({}) has no contender registered.",
                    sessionID,
                    leaderElectionDriver);
            return;
        } else if (!sessionID.equals(issuedLeaderSessionID)) {
            LOG.debug(
                    "An out-dated leadership-acquired event with session ID {} was triggered. The current leader session ID is {}. The event will be ignored.",
                    sessionID,
                    issuedLeaderSessionID);
            return;
        }

        Preconditions.checkState(
                !confirmedLeaderInformation.hasLeaderInformation(componentId),
                "The leadership should have been granted while not having the leadership acquired.");

        LOG.debug(
                "Granting leadership to the contender registered under component '{}' with session ID {}.",
                componentId,
                issuedLeaderSessionID);

        leaderContenderRegistry.get(componentId).grantLeadership(issuedLeaderSessionID);
    }

    @GuardedBy("lock")
    private void onRevokeLeadershipInternal() {
        Preconditions.checkState(
                issuedLeaderSessionID != null,
                "The leadership should have been revoked while having the leadership acquired.");

        if (!leaderContenderRegistry.isEmpty()) {
            leaderContenderRegistry.forEach(this::notifyLeaderContenderOfLeadershipLoss);
        } else {
            LOG.debug(
                    "The revoke leadership notification for session {} is not forwarded because the DefaultLeaderElectionService({}) has no contender registered.",
                    issuedLeaderSessionID,
                    leaderElectionDriver);
        }

        issuedLeaderSessionID = null;
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadershipLoss(
            String componentId, LeaderContender leaderContender) {
        Preconditions.checkState(
                leaderContender != null,
                "The LeaderContender should be always set when calling this method.");

        if (!confirmedLeaderInformation.hasLeaderInformation(componentId)) {
            LOG.debug(
                    "Revoking leadership for component '{}' while a previous leadership grant wasn't confirmed, yet.",
                    componentId);
        } else {
            LOG.debug(
                    "Revoking leadership to component '{}' for previously confirmed leader information {}.",
                    componentId,
                    LeaderElectionUtils.convertToString(
                            confirmedLeaderInformation.forComponentIdOrEmpty(componentId)));
        }

        confirmedLeaderInformation =
                LeaderInformationRegister.clear(confirmedLeaderInformation, componentId);
        leaderContender.revokeLeadership();
    }

    @GuardedBy("lock")
    private void notifyLeaderInformationChangeInternal(
            String componentId,
            LeaderInformation externallyChangedLeaderInformation,
            LeaderInformation confirmedLeaderInformation) {
        if (leaderElectionDriver == null) {
            LOG.debug(
                    "The LeaderElectionDriver was disconnected. Any incoming events will be ignored.");
            return;
        }

        if (confirmedLeaderInformation.equals(externallyChangedLeaderInformation)) {
            LOG.trace(
                    "LeaderInformation change event received but changed LeaderInformation actually matches the locally confirmed one: {}",
                    confirmedLeaderInformation);
            return;
        }

        if (confirmedLeaderInformation.isEmpty()) {
            LOG.trace(
                    "Leader information changed while there's no confirmation available by the contender for component '{}', yet. Changed leader information {} will be reset.",
                    componentId,
                    LeaderElectionUtils.convertToString(externallyChangedLeaderInformation));
        } else if (externallyChangedLeaderInformation.isEmpty()) {
            LOG.debug(
                    "Re-writing leader information ({}) for component '{}' to overwrite the empty leader information in the external storage.",
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    componentId);
        } else {
            // the changed LeaderInformation does not match the confirmed LeaderInformation
            LOG.debug(
                    "Correcting leader information for component '{}' (local: {}, external storage: {}).",
                    componentId,
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    LeaderElectionUtils.convertToString(externallyChangedLeaderInformation));
        }

        leaderElectionDriver.publishLeaderInformation(componentId, confirmedLeaderInformation);
    }

    private void runInLeaderEventThread(String leaderElectionEventName, Runnable callback) {
        synchronized (lock) {
            if (running) {
                LOG.debug("'{}' event processing triggered.", leaderElectionEventName);
                FutureUtils.handleUncaughtException(
                        CompletableFuture.runAsync(
                                () -> {
                                    synchronized (lock) {
                                        if (!running) {
                                            LOG.debug(
                                                    "Processing '{}' event omitted due to the service not being in running state, anymore.",
                                                    leaderElectionEventName);
                                        } else if (leaderElectionDriver == null) {
                                            Preconditions.checkState(
                                                    leaderContenderRegistry.isEmpty(),
                                                    "All contenders should be deregistered when the driver is removed.");
                                            LOG.debug(
                                                    "All contenders have been deregistered and the driver was shut down. Any incoming leadership event will be ignored.");
                                        } else {
                                            LOG.debug(
                                                    "Processing '{}' event.",
                                                    leaderElectionEventName);
                                            callback.run();
                                        }
                                    }
                                },
                                leadershipOperationExecutor),
                        (thread, error) -> forwardErrorToLeaderContender(error));
            } else {
                LOG.debug(
                        "'{}' event processing was triggered while the DefaultLeaderElectionService is closed. The event will be ignored.",
                        leaderElectionEventName);
            }
        }
    }

    private void forwardErrorToLeaderContender(Throwable t) {
        synchronized (lock) {
            if (leaderContenderRegistry.isEmpty()) {
                fallbackErrorHandler.onFatalError(t);
                return;
            }

            leaderContenderRegistry
                    .values()
                    .forEach(
                            leaderContender -> {
                                if (t instanceof LeaderElectionException) {
                                    leaderContender.handleError((LeaderElectionException) t);
                                } else {
                                    leaderContender.handleError(new LeaderElectionException(t));
                                }
                            });
        }
    }

    @Override
    public void onGrantLeadership(UUID leaderSessionID) {
        runInLeaderEventThread(
                LEADER_ACQUISITION_EVENT_LOG_NAME,
                () -> onGrantLeadershipInternal(leaderSessionID));
    }

    @Override
    public void onRevokeLeadership() {
        runInLeaderEventThread(LEADER_REVOCATION_EVENT_LOG_NAME, this::onRevokeLeadershipInternal);
    }

    @Override
    public void onLeaderInformationChange(String componentId, LeaderInformation leaderInformation) {
        synchronized (lock) {
            notifyLeaderInformationChangeInternal(
                    componentId,
                    leaderInformation,
                    confirmedLeaderInformation.forComponentIdOrEmpty(componentId));
        }
    }

    @Override
    public void onLeaderInformationChange(LeaderInformationRegister changedLeaderInformation) {
        synchronized (lock) {
            leaderContenderRegistry.forEach(
                    (componentId, leaderContender) -> {
                        final LeaderInformation externallyChangedLeaderInformationForContender =
                                changedLeaderInformation
                                        .forComponentId(componentId)
                                        .orElse(LeaderInformation.empty());
                        final LeaderInformation confirmedLeaderInformationForContender =
                                confirmedLeaderInformation.forComponentIdOrEmpty(componentId);

                        notifyLeaderInformationChangeInternal(
                                componentId,
                                externallyChangedLeaderInformationForContender,
                                confirmedLeaderInformationForContender);
                    });
        }
    }

    @Override
    public void onError(Throwable t) {
        forwardErrorToLeaderContender(t);
    }
}
