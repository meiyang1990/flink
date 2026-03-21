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

import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This utility class implements the basis of registering one component at another component, for
 * example registering the TaskExecutor at the ResourceManager. This {@code RetryingRegistration}
 * implements both the initial address resolution and the retries-with-backoff strategy.
 *
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * registration can be canceled, for example when the target where it tries to register at loses
 * leader status.
 *
 * <p>【学习型注释】RetryingRegistration 实现了组件间的重试注册机制，是 Flink 容错设计的重要组成部分。
 *
 * <h2>典型使用场景</h2>
 * <ul>
 *   <li>TaskExecutor 向 ResourceManager 注册</li>
 *   <li>TaskExecutor 向 JobMaster 注册</li>
 *   <li>JobMaster 向 ResourceManager 注册</li>
 * </ul>
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li><b>地址解析</b>：通过 RpcService 将目标地址解析为 RpcGateway</li>
 *   <li><b>超时重试</b>：支持指数退避的超时重试策略</li>
 *   <li><b>失败重试</b>：连接失败或注册被拒绝时自动重试</li>
 *   <li><b>可取消</b>：支持通过 cancel() 取消正在进行的注册</li>
 * </ul>
 *
 * <h2>重试策略</h2>
 * <ol>
 *   <li>初始超时时间较短（快速检测可用性）</li>
 *   <li>每次超时后，超时时间翻倍，直到达到最大超时</li>
 *   <li>发生错误时，固定延迟后重试</li>
 *   <li>被拒绝时，固定延迟后重试（可能是 Leader 变更）</li>
 * </ol>
 *
 * <h2>状态转换</h2>
 * <pre>
 * startRegistration() → 地址解析 → register() → 成功/拒绝/失败
 *                          ↓                        ↓
 *                      重试（错误延迟）          重试（拒绝延迟）
 * </pre>
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 * @param <R> The type of the registration rejection responses.
 */
public abstract class RetryingRegistration<
        F extends Serializable,
        G extends RpcGateway,
        S extends RegistrationResponse.Success,
        R extends RegistrationResponse.Rejection> {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    /** 日志记录器，由子类提供 */
    private final Logger log;

    /** RPC 服务，用于连接远程 Gateway */
    private final RpcService rpcService;

    /** 注册目标名称（用于日志），如 "ResourceManager" */
    private final String targetName;

    /** 目标 Gateway 的类型 */
    private final Class<G> targetType;

    /** 目标 RPC 地址 */
    private final String targetAddress;

    /**
     * 围栏令牌，用于 FencedRpcGateway。
     * 确保只与正确的 Leader 通信，防止脑裂场景下的消息错发。
     */
    private final F fencingToken;

    /**
     * 注册完成的 Future，包含注册结果（成功/拒绝）。
     */
    private final CompletableFuture<RetryingRegistrationResult<G, S, R>> completionFuture;

    /**
     * 重试注册的配置参数（初始超时、最大超时、错误延迟、拒绝延迟等）。
     */
    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    /**
     * 取消标志，volatile 保证多线程可见性。
     */
    private volatile boolean canceled;

    // ------------------------------------------------------------------------

    public RetryingRegistration(
            Logger log,
            RpcService rpcService,
            String targetName,
            Class<G> targetType,
            String targetAddress,
            F fencingToken,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.log = checkNotNull(log);
        this.rpcService = checkNotNull(rpcService);
        this.targetName = checkNotNull(targetName);
        this.targetType = checkNotNull(targetType);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.completionFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  completion and cancellation
    // ------------------------------------------------------------------------

    public CompletableFuture<RetryingRegistrationResult<G, S, R>> getFuture() {
        return completionFuture;
    }

    /** Cancels the registration procedure. */
    public void cancel() {
        canceled = true;
        completionFuture.cancel(false);
    }

    /**
     * Checks if the registration was canceled.
     *
     * @return True if the registration was canceled, false otherwise.
     */
    public boolean isCanceled() {
        return canceled;
    }

    // ------------------------------------------------------------------------
    //  registration
    // ------------------------------------------------------------------------

    protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(
            G gateway, F fencingToken, long timeoutMillis) throws Exception;

    /**
     * This method resolves the target address to a callable gateway and starts the registration
     * after that.
     *
     * <p>【学习型注释】启动注册流程。这是注册的入口方法。
     *
     * <p>流程：
     * <ol>
     *   <li>检查是否已取消</li>
     *   <li>通过 RpcService 将目标地址解析为 RpcGateway</li>
     *   <li>解析成功后，开始实际的注册尝试（调用 register 方法）</li>
     *   <li>解析失败时，延迟后重试地址解析</li>
     * </ol>
     *
     * <p>特殊处理：如果目标是 FencedRpcGateway，连接时会携带围栏令牌。
     */
    @SuppressWarnings("unchecked")
    public void startRegistration() {
        if (canceled) {
            // we already got canceled
            return;
        }

        try {
            // trigger resolution of the target address to a callable gateway
            final CompletableFuture<G> rpcGatewayFuture;

            if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
                rpcGatewayFuture =
                        (CompletableFuture<G>)
                                rpcService.connect(
                                        targetAddress,
                                        fencingToken,
                                        targetType.asSubclass(FencedRpcGateway.class));
            } else {
                rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
            }

            // upon success, start the registration attempts
            CompletableFuture<Void> rpcGatewayAcceptFuture =
                    rpcGatewayFuture.thenAcceptAsync(
                            (G rpcGateway) -> {
                                log.info("Resolved {} address, beginning registration", targetName);
                                register(
                                        rpcGateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis());
                            },
                            rpcService.getScheduledExecutor());

            // upon failure, retry, unless this is cancelled
            rpcGatewayAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        if (failure != null && !canceled) {
                            final Throwable strippedFailure =
                                    ExceptionUtils.stripCompletionException(failure);
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Could not resolve {} address {}, retrying in {} ms.",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure);
                            } else {
                                log.info(
                                        "Could not resolve {} address {}, retrying in {} ms: {}",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure.getMessage());
                            }

                            startRegistrationLater(
                                    retryingRegistrationConfiguration.getErrorDelayMillis());
                        }
                    },
                    rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    /**
     * This method performs a registration attempt and triggers either a success notification or a
     * retry, depending on the result.
     *
     * <p>【学习型注释】执行单次注册尝试。根据结果决定是完成注册还是重试。
     *
     * <p>注册结果处理：
     * <ul>
     *   <li><b>Success</b>：注册成功，完成 completionFuture</li>
     *   <li><b>Rejection</b>：被拒绝（如 Leader 变更），完成 Future 但标记为拒绝</li>
     *   <li><b>Failure</b>：失败，延迟后以初始超时重新开始注册</li>
     *   <li><b>Timeout</b>：超时，超时时间翻倍后立即重试</li>
     *   <li><b>Exception</b>：异常，延迟后重试</li>
     * </ul>
     *
     * <p>超时策略：初始超时 → 2×超时 → 4×超时 → ... → 最大超时
     */
    @SuppressWarnings("unchecked")
    private void register(final G gateway, final int attempt, final long timeoutMillis) {
        // eager check for canceling to avoid some unnecessary work
        if (canceled) {
            return;
        }

        try {
            log.debug(
                    "Registration at {} attempt {} (timeout={}ms)",
                    targetName,
                    attempt,
                    timeoutMillis);
            CompletableFuture<RegistrationResponse> registrationFuture =
                    invokeRegistration(gateway, fencingToken, timeoutMillis);

            // if the registration was successful, let the TaskExecutor know
            CompletableFuture<Void> registrationAcceptFuture =
                    registrationFuture.thenAcceptAsync(
                            (RegistrationResponse result) -> {
                                if (!isCanceled()) {
                                    if (result instanceof RegistrationResponse.Success) {
                                        log.debug(
                                                "Registration with {} at {} was successful.",
                                                targetName,
                                                targetAddress);
                                        S success = (S) result;
                                        completionFuture.complete(
                                                RetryingRegistrationResult.success(
                                                        gateway, success));
                                    } else if (result instanceof RegistrationResponse.Rejection) {
                                        log.debug(
                                                "Registration with {} at {} was rejected.",
                                                targetName,
                                                targetAddress);
                                        R rejection = (R) result;
                                        completionFuture.complete(
                                                RetryingRegistrationResult.rejection(rejection));
                                    } else {
                                        // registration failure
                                        if (result instanceof RegistrationResponse.Failure) {
                                            RegistrationResponse.Failure failure =
                                                    (RegistrationResponse.Failure) result;
                                            log.info(
                                                    "Registration failure at {} occurred.",
                                                    targetName,
                                                    failure.getReason());
                                        } else {
                                            log.error(
                                                    "Received unknown response to registration attempt: {}",
                                                    result);
                                        }

                                        log.info(
                                                "Pausing and re-attempting registration in {} ms",
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                        registerLater(
                                                gateway,
                                                1,
                                                retryingRegistrationConfiguration
                                                        .getInitialRegistrationTimeoutMillis(),
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                    }
                                }
                            },
                            rpcService.getScheduledExecutor());

            // upon failure, retry
            registrationAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        if (failure != null && !isCanceled()) {
                            if (ExceptionUtils.stripCompletionException(failure)
                                    instanceof TimeoutException) {
                                // we simply have not received a response in time. maybe the timeout
                                // was
                                // very low (initial fast registration attempts), maybe the target
                                // endpoint is
                                // currently down.
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Registration at {} ({}) attempt {} timed out after {} ms",
                                            targetName,
                                            targetAddress,
                                            attempt,
                                            timeoutMillis);
                                }

                                long newTimeoutMillis =
                                        Math.min(
                                                2 * timeoutMillis,
                                                retryingRegistrationConfiguration
                                                        .getMaxRegistrationTimeoutMillis());
                                register(gateway, attempt + 1, newTimeoutMillis);
                            } else {
                                // a serious failure occurred. we still should not give up, but keep
                                // trying
                                log.error(
                                        "Registration at {} failed due to an error",
                                        targetName,
                                        failure);
                                log.info(
                                        "Pausing and re-attempting registration in {} ms",
                                        retryingRegistrationConfiguration.getErrorDelayMillis());

                                registerLater(
                                        gateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis(),
                                        retryingRegistrationConfiguration.getErrorDelayMillis());
                            }
                        }
                    },
                    rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    private void registerLater(
            final G gateway, final int attempt, final long timeoutMillis, long delay) {
        rpcService
                .getScheduledExecutor()
                .schedule(
                        () -> register(gateway, attempt, timeoutMillis),
                        delay,
                        TimeUnit.MILLISECONDS);
    }

    private void startRegistrationLater(final long delay) {
        rpcService
                .getScheduledExecutor()
                .schedule(this::startRegistration, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 注册结果封装类，表示注册成功或被拒绝。
     *
     * <p>【学习型注释】使用工厂方法 success() 和 rejection() 创建实例。
     * <ul>
     *   <li>成功时：gateway 和 success 非空，rejection 为空</li>
     *   <li>被拒绝时：rejection 非空，gateway 和 success 为空</li>
     * </ul>
     */
    static final class RetryingRegistrationResult<G, S, R> {
        /** 成功时的 RPC Gateway 引用 */
        @Nullable private final G gateway;

        /** 成功响应内容 */
        @Nullable private final S success;

        /** 拒绝响应内容 */
        @Nullable private final R rejection;

        private RetryingRegistrationResult(
                @Nullable G gateway, @Nullable S success, @Nullable R rejection) {
            this.gateway = gateway;
            this.success = success;
            this.rejection = rejection;
        }

        boolean isSuccess() {
            return success != null && gateway != null;
        }

        boolean isRejection() {
            return rejection != null;
        }

        public G getGateway() {
            Preconditions.checkState(isSuccess());
            return gateway;
        }

        public R getRejection() {
            Preconditions.checkState(isRejection());
            return rejection;
        }

        public S getSuccess() {
            Preconditions.checkState(isSuccess());
            return success;
        }

        static <
                        G extends RpcGateway,
                        S extends RegistrationResponse.Success,
                        R extends RegistrationResponse.Rejection>
                RetryingRegistrationResult<G, S, R> success(G gateway, S success) {
            return new RetryingRegistrationResult<>(gateway, success, null);
        }

        static <
                        G extends RpcGateway,
                        S extends RegistrationResponse.Success,
                        R extends RegistrationResponse.Rejection>
                RetryingRegistrationResult<G, S, R> rejection(R rejection) {
            return new RetryingRegistrationResult<>(null, null, rejection);
        }
    }
}
