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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.rpc.RpcGateway;

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This utility class implements the basis of RPC connecting from one component to another
 * component, for example the RPC connection from TaskExecutor to ResourceManager. This {@code
 * RegisteredRpcConnection} implements registration and get target gateway.
 *
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * RPC connection can be closed, for example when the target where it tries to register loses leader
 * status.
 *
 * <h2>RPC 注册连接管理器 - 中文说明</h2>
 *
 * <p>本类是 Flink 分布式组件之间 RPC 连接注册的基础抽象，实现了从一个组件向另一个组件发起连接并完成注册的核心流程。
 *
 * <h3>典型使用场景</h3>
 * <ul>
 *   <li><b>TaskExecutor → ResourceManager</b>：TaskManager 启动后向 ResourceManager 注册自身</li>
 *   <li><b>JobMaster → ResourceManager</b>：作业启动时 JobMaster 向 ResourceManager 请求资源</li>
 *   <li><b>TaskExecutor → JobMaster</b>：TaskManager 向 JobMaster 注册以接收任务部署</li>
 * </ul>
 *
 * <h3>核心设计要点</h3>
 * <ul>
 *   <li><b>Fencing Token 机制</b>：使用 fencing token 防止脑裂场景下的旧 leader 干扰</li>
 *   <li><b>带重试的异步注册</b>：内部使用 {@link RetryingRegistration} 实现指数退避重试</li>
 *   <li><b>并发安全</b>：使用 AtomicReferenceFieldUpdater 保证注册状态的原子更新</li>
 *   <li><b>可中断连接</b>：支持在目标组件 leader 切换时关闭连接并重新注册</li>
 * </ul>
 *
 * <h3>状态转换</h3>
 * <pre>
 *  初始状态 → start() → 注册中 → 注册成功 → 已连接
 *                  ↓
 *               注册失败/被拒绝 → 回调通知
 *                  ↓
 *               close() → 已关闭
 * </pre>
 *
 * <h3>子类需要实现的抽象方法</h3>
 * <ul>
 *   <li>{@link #generateRegistration()}：创建具体的注册实例</li>
 *   <li>{@link #onRegistrationSuccess(RegistrationResponse.Success)}：处理注册成功响应</li>
 *   <li>{@link #onRegistrationRejection(RegistrationResponse.Rejection)}：处理注册被拒绝</li>
 *   <li>{@link #onRegistrationFailure(Throwable)}：处理注册异常</li>
 * </ul>
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 * @param <R> The type of the registration rejection responses.
 */
public abstract class RegisteredRpcConnection<
        F extends Serializable,
        G extends RpcGateway,
        S extends RegistrationResponse.Success,
        R extends RegistrationResponse.Rejection> {

    /**
     * 原子字段更新器，用于线程安全地更新 pendingRegistration 字段。
     * 保证在并发场景下（如同时调用 start 和 tryReconnect）注册状态的一致性。
     */
    private static final AtomicReferenceFieldUpdater<RegisteredRpcConnection, RetryingRegistration>
            REGISTRATION_UPDATER =
                    AtomicReferenceFieldUpdater.newUpdater(
                            RegisteredRpcConnection.class,
                            RetryingRegistration.class,
                            "pendingRegistration");

    /** The logger for all log messages of this class. */
    protected final Logger log;

    /**
     * 远程组件的 Fencing Token（隔离令牌）。
     * 用于确保只与正确的 leader 通信，防止脑裂场景下连接到旧 leader。
     */
    private final F fencingToken;

    /**
     * 目标组件的 RPC 地址。
     * 例如 ResourceManager 的 akka 地址：akka.tcp://flink@host:port/user/rpc/resourcemanager
     */
    private final String targetAddress;

    /**
     * 执行器，用于异步执行注册完成后的回调。
     * 通常是组件的主线程执行器，确保回调在正确的线程上下文中执行。
     */
    private final Executor executor;

    /**
     * 当前正在进行的注册实例。
     * volatile 保证多线程可见性，配合 REGISTRATION_UPDATER 实现无锁更新。
     * 注册成功后此字段仍然保留，用于 tryReconnect 时取消旧注册。
     */
    private volatile RetryingRegistration<F, G, S, R> pendingRegistration;

    /**
     * 注册成功后获取到的目标网关。
     * 在注册完成前为 null，完成后可通过此网关与目标组件进行 RPC 调用。
     */
    private volatile G targetGateway;

    /**
     * 连接关闭标志。
     * 一旦置为 true，所有后续操作（start、tryReconnect）都将被拒绝。
     */
    private volatile boolean closed;

    // ------------------------------------------------------------------------

    public RegisteredRpcConnection(
            Logger log, String targetAddress, F fencingToken, Executor executor) {
        this.log = checkNotNull(log);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.executor = checkNotNull(executor);
    }

    // ------------------------------------------------------------------------
    //  Life cycle - 生命周期管理
    // ------------------------------------------------------------------------

    /**
     * 启动 RPC 连接注册流程。
     *
     * <p>此方法创建一个新的 {@link RetryingRegistration} 实例并开始异步注册。
     * 使用 CAS 操作确保只有一个注册实例能够启动，防止并发调用导致的重复注册。
     *
     * <p>调用前提：连接未关闭且未启动过
     */
    public void start() {
        checkState(!closed, "The RPC connection is already closed");
        checkState(
                !isConnected() && pendingRegistration == null,
                "The RPC connection is already started");

        // 创建新的注册实例
        final RetryingRegistration<F, G, S, R> newRegistration = createNewRegistration();

        // CAS 更新：只有当 pendingRegistration 为 null 时才设置新值
        if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
            newRegistration.startRegistration();
        } else {
            // 并发启动场景：其他线程已经设置了注册实例，取消本次创建的注册
            newRegistration.cancel();
        }
    }

    /**
     * Tries to reconnect to the {@link #targetAddress} by cancelling the pending registration and
     * starting a new pending registration.
     *
     * <p>尝试重新连接到目标地址。此方法会取消当前进行中的注册并启动新的注册流程。
     * 典型使用场景：当与目标组件的连接出现问题时（如心跳超时、RPC 失败），触发重连。
     *
     * <p>实现要点：
     * <ul>
     *   <li>使用 CAS 保证并发安全</li>
     *   <li>双重检查 closed 标志，防止在更新过程中被关闭</li>
     *   <li>失败时返回 false，调用方可据此决定后续处理</li>
     * </ul>
     *
     * @return {@code false} if the connection has been closed or a concurrent modification has
     *     happened; otherwise {@code true}
     */
    public boolean tryReconnect() {
        checkState(isConnected(), "Cannot reconnect to an unknown destination.");

        if (closed) {
            return false;
        } else {
            final RetryingRegistration<F, G, S, R> currentPendingRegistration = pendingRegistration;

            // 取消当前正在进行的注册（如果有的话）
            if (currentPendingRegistration != null) {
                currentPendingRegistration.cancel();
            }

            // 创建并尝试设置新的注册实例
            final RetryingRegistration<F, G, S, R> newRegistration = createNewRegistration();

            if (REGISTRATION_UPDATER.compareAndSet(
                    this, currentPendingRegistration, newRegistration)) {
                newRegistration.startRegistration();
            } else {
                // 并发修改：其他线程已经更新了注册实例
                newRegistration.cancel();
                return false;
            }

            // 双重检查：防止在 CAS 成功后被并发关闭
            if (closed) {
                newRegistration.cancel();

                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * This method generate a specific Registration, for example TaskExecutor Registration at the
     * ResourceManager.
     *
     * <p>生成特定类型的注册实例。子类实现此方法以创建具体的注册逻辑，例如：
     * <ul>
     *   <li>TaskExecutor 向 ResourceManager 的注册</li>
     *   <li>JobMaster 向 ResourceManager 的注册</li>
     * </ul>
     */
    protected abstract RetryingRegistration<F, G, S, R> generateRegistration();

    /**
     * This method handle the Registration Response.
     *
     * <p>处理注册成功的响应。子类在此方法中执行注册成功后的业务逻辑，例如：
     * <ul>
     *   <li>保存分配的资源 ID</li>
     *   <li>启动心跳监控</li>
     *   <li>更新组件状态</li>
     * </ul>
     */
    protected abstract void onRegistrationSuccess(S success);

    /**
     * This method handles the Registration rejection.
     *
     * <p>处理注册被拒绝的情况。被拒绝通常表示目标组件因业务原因无法接受注册，
     * 例如资源不足、版本不兼容等。子类根据拒绝原因决定后续处理。
     *
     * @param rejection rejection containing additional information about the rejection
     */
    protected abstract void onRegistrationRejection(R rejection);

    /**
     * This method handle the Registration failure.
     *
     * <p>处理注册过程中的异常。与 rejection 不同，failure 通常是由于网络、超时等
     * 技术原因导致的注册失败。子类可选择重试或上报错误。
     */
    protected abstract void onRegistrationFailure(Throwable failure);

    /**
     * Close connection.
     *
     * <p>关闭 RPC 连接。设置 closed 标志并取消正在进行的注册。
     * 关闭后不能再调用 start() 或 tryReconnect()。
     */
    public void close() {
        closed = true;

        // 确保不会无限重试
        if (pendingRegistration != null) {
            pendingRegistration.cancel();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public F getTargetLeaderId() {
        return fencingToken;
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    /** Gets the RegisteredGateway. This returns null until the registration is completed. */
    public G getTargetGateway() {
        return targetGateway;
    }

    public boolean isConnected() {
        return targetGateway != null;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        String connectionInfo =
                "(ADDRESS: " + targetAddress + " FENCINGTOKEN: " + fencingToken + ")";

        if (isConnected()) {
            connectionInfo =
                    "RPC connection to "
                            + targetGateway.getClass().getSimpleName()
                            + " "
                            + connectionInfo;
        } else {
            connectionInfo = "RPC connection to " + connectionInfo;
        }

        if (isClosed()) {
            connectionInfo += " is closed";
        } else if (isConnected()) {
            connectionInfo += " is established";
        } else {
            connectionInfo += " is connecting";
        }

        return connectionInfo;
    }

    // ------------------------------------------------------------------------
    //  Internal methods - 内部实现方法
    // ------------------------------------------------------------------------

    /**
     * 创建新的注册实例并设置完成回调。
     *
     * <p>此方法是注册流程的核心，执行以下步骤：
     * <ol>
     *   <li>调用 {@link #generateRegistration()} 创建具体的注册实例</li>
     *   <li>获取注册的 Future 并注册完成回调</li>
     *   <li>在回调中根据结果类型调用相应的处理方法</li>
     * </ol>
     *
     * <p>注册结果的三种可能：
     * <ul>
     *   <li><b>Success</b>：注册成功，保存 gateway 并调用 onRegistrationSuccess</li>
     *   <li><b>Rejection</b>：注册被拒绝，调用 onRegistrationRejection</li>
     *   <li><b>Failure</b>：注册异常，区分取消异常和其他异常</li>
     * </ul>
     */
    private RetryingRegistration<F, G, S, R> createNewRegistration() {
        RetryingRegistration<F, G, S, R> newRegistration = checkNotNull(generateRegistration());

        CompletableFuture<RetryingRegistration.RetryingRegistrationResult<G, S, R>> future =
                newRegistration.getFuture();

        // 注册完成回调，在 executor 线程上异步执行
        future.whenCompleteAsync(
                (RetryingRegistration.RetryingRegistrationResult<G, S, R> result,
                        Throwable failure) -> {
                    if (failure != null) {
                        // 区分取消异常和其他异常
                        if (failure instanceof CancellationException) {
                            // 取消异常是主动触发的（调用 cancel），仅记录 debug 日志
                            log.debug(
                                    "Retrying registration towards {} was cancelled.",
                                    targetAddress);
                        } else {
                            // 其他异常表示注册过程出现问题，通知子类处理
                            onRegistrationFailure(failure);
                        }
                    } else {
                        // 根据注册结果类型分发处理
                        if (result.isSuccess()) {
                            // 注册成功：保存目标网关，后续可通过此网关进行 RPC 调用
                            targetGateway = result.getGateway();
                            onRegistrationSuccess(result.getSuccess());
                        } else if (result.isRejection()) {
                            // 注册被拒绝：目标组件主动拒绝了注册请求
                            onRegistrationRejection(result.getRejection());
                        } else {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Unknown retrying registration response: %s.", result));
                        }
                    }
                },
                executor);

        return newRegistration;
    }
}
