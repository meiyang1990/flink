// 这个文件已经全部加上中文注释
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

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** 
 * Base class for all applications. 
 * <p>【学习型注释】所有 Flink 应用的基类，负责维护应用的状态机、生命周期时间戳、异常历史以及作业列表。
 */
public abstract class AbstractApplication implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractApplication.class);

    private static final long serialVersionUID = 1L;

    private final ApplicationID applicationId;

    private ApplicationState applicationState;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()}) when the
     * application transitioned into a certain status. The index into this array is the ordinal of
     * the enum value, i.e. the timestamp when the application went into state "RUNNING" is at
     * {@code timestamps[RUNNING.ordinal()]}.
     * <p>【学习型注释】记录应用转换到各个状态的时间戳，用于监控和统计应用的耗时分布。
     */
    private final long[] statusTimestamps;

    private final Set<JobID> jobs = new HashSet<>();

    private final List<ApplicationExceptionHistoryEntry> exceptionHistory = new ArrayList<>();

    /**
     * List of registered application status listeners that will be notified via {@link
     * ApplicationStatusListener#notifyApplicationStatusChange} when the application state changes.
     * For example, the Dispatcher registers itself as a listener to perform operations such as
     * archiving when the application reaches a terminal state.
     * <p>【学习型注释】注册的应用状态监听器列表，当应用状态发生变化时通知它们。例如，Dispatcher 注册自己以在应用达到终态时执行归档。
     */
    private transient List<ApplicationStatusListener> statusListeners = new ArrayList<>();

    public AbstractApplication(ApplicationID applicationId) {
        this.applicationId = checkNotNull(applicationId);
        this.statusTimestamps = new long[ApplicationState.values().length];
        this.applicationState = ApplicationState.CREATED;
        this.statusTimestamps[ApplicationState.CREATED.ordinal()] = System.currentTimeMillis();
    }

    /**
     * Entry method to run the application asynchronously.
     *
     * <p>The returned CompletableFuture indicates that the execution request has been accepted and
     * the application transitions to RUNNING state.
     *
     * <p><b>Note:</b> This method must be called in the main thread of the {@link Dispatcher}.
     *
     * @param dispatcherGateway the dispatcher of the cluster to run the application.
     * @param scheduledExecutor the executor to run the user logic.
     * @param mainThreadExecutor the executor bound to the main thread.
     * @param errorHandler the handler for fatal errors.
     * @return a future indicating that the execution request has been accepted.
     * <p>【学习型注释】异步运行应用的入口方法，要求在 Dispatcher 主线程执行。
     */
    public abstract CompletableFuture<Acknowledge> execute(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler);

    /**
     * Cancels the application execution.
     *
     * <p>This method is responsible for initiating the cancellation process and handling the
     * appropriate state transitions of the application.
     *
     * <p><b>Note:</b> This method must be called in the main thread of the {@link Dispatcher}.
     * <p>【学习型注释】取消应用执行，负责触发取消流程并处理状态转换，要求在 Dispatcher 主线程执行。
     */
    public abstract void cancel();

    /**
     * Cleans up execution associated with the application.
     *
     * <p>This method is typically invoked when the cluster is shutting down.
     * <p>【学习型注释】清理与应用相关的资源，通常在集群关闭时调用。
     */
    public abstract void dispose();

    public abstract String getName();

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public Set<JobID> getJobs() {
        return Collections.unmodifiableSet(jobs);
    }

    public List<ApplicationExceptionHistoryEntry> getExceptionHistory() {
        return Collections.unmodifiableList(exceptionHistory);
    }

    public void addExceptionHistoryEntry(Throwable throwable, @Nullable JobID jobId) {
        exceptionHistory.add(
                new ApplicationExceptionHistoryEntry(throwable, System.currentTimeMillis(), jobId));
    }

    /**
     * Adds a job ID to the jobs set.
     *
     * <p><b>Note:</b>This method must be called in the main thread of the {@link Dispatcher}.
     * <p>【学习型注释】将作业 ID 添加到应用关联的作业集合中，要求在 Dispatcher 主线程执行。
     */
    public boolean addJob(JobID jobId) {
        return jobs.add(jobId);
    }

    public ApplicationState getApplicationStatus() {
        return applicationState;
    }

    /**
     * Registers a status listener.
     *
     * <p>This method is not thread-safe and should not be called concurrently.
     * <p>【学习型注释】注册状态监听器，非线程安全方法。
     */
    public void registerStatusListener(ApplicationStatusListener listener) {
        getStatusListeners().add(listener);
    }

    private List<ApplicationStatusListener> getStatusListeners() {
        if (statusListeners == null) {
            statusListeners = new ArrayList<>();
        }
        return statusListeners;
    }

    // ------------------------------------------------------------------------
    //  State Transitions
    // ------------------------------------------------------------------------

    private static final Map<ApplicationState, Set<ApplicationState>> ALLOWED_TRANSITIONS;

    static {
        ALLOWED_TRANSITIONS = new EnumMap<>(ApplicationState.class);
        ALLOWED_TRANSITIONS.put(
                ApplicationState.CREATED,
                new HashSet<>(Arrays.asList(ApplicationState.RUNNING, ApplicationState.CANCELING)));
        ALLOWED_TRANSITIONS.put(
                ApplicationState.RUNNING,
                new HashSet<>(
                        Arrays.asList(
                                ApplicationState.FINISHED,
                                ApplicationState.FAILING,
                                ApplicationState.CANCELING)));
        ALLOWED_TRANSITIONS.put(
                ApplicationState.FAILING,
                new HashSet<>(Collections.singletonList(ApplicationState.FAILED)));
        ALLOWED_TRANSITIONS.put(
                ApplicationState.CANCELING,
                new HashSet<>(Collections.singletonList(ApplicationState.CANCELED)));
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToRunning() {
        transitionState(ApplicationState.RUNNING);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToCanceling() {
        transitionState(ApplicationState.CANCELING);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToFailing() {
        transitionState(ApplicationState.FAILING);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToFailed() {
        transitionState(ApplicationState.FAILED);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToFinished() {
        transitionState(ApplicationState.FINISHED);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToCanceled() {
        transitionState(ApplicationState.CANCELED);
    }

    void transitionState(ApplicationState targetState) {
        validateTransition(targetState);
        LOG.info(
                "Application {} ({}) switched from state {} to {}.",
                getName(),
                getApplicationId(),
                applicationState,
                targetState);
        this.statusTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        this.applicationState = targetState;
        // 【学习型注释】状态变更后通知所有监听器，触发归档等后续操作
        getStatusListeners()
                .forEach(
                        listener ->
                                listener.notifyApplicationStatusChange(applicationId, targetState));
    }

    // 【学习型注释】根据 ALLOWED_TRANSITIONS 静态映射校验目标状态是否合法，防止非法的状态流转
    private void validateTransition(ApplicationState targetState) {
        Set<ApplicationState> allowedTransitions = ALLOWED_TRANSITIONS.get(applicationState);
        if (allowedTransitions == null || !allowedTransitions.contains(targetState)) {
            throw new IllegalStateException(
                    String.format(
                            "Invalid transition from %s to %s", applicationState, targetState));
        }
    }

    public long getStatusTimestamp(ApplicationState status) {
        return this.statusTimestamps[status.ordinal()];
    }
}
