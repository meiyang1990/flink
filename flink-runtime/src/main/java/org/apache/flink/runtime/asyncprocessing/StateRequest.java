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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A request encapsulates the necessary data to perform a state request.
 *
 * @param <K> Type of partitioned key.
 * @param <IN> Type of input of this request.
 * @param <N> Type of namespace.
 * @param <OUT> Type of value that request will return.
 */
// 【学习型注释】状态请求的具体实现，封装了目标 State、请求类型、输入载荷、命名空间和结果 Future。
// 构造时从 RecordContext 中提取当前 namespace，确保请求携带完整的上下文信息。
// 实现 Serializable 以支持在网络中传输（如远程状态访问场景）。
public class StateRequest<K, N, IN, OUT> extends AsyncRequest<K> implements Serializable {

    /** The underlying state to be accessed. */
    @Nonnull private final State state;

    /** The type of this request. */
    private final StateRequestType type;

    /** The payload(input) of this request. */
    @Nullable private final IN payload;

    @Nonnull private final N namespace;

    public StateRequest(
            @Nonnull State state,
            StateRequestType type,
            boolean sync,
            @Nullable IN payload,
            InternalAsyncFuture<OUT> stateFuture,
            RecordContext<K> context) {
        super(context, sync, stateFuture);
        this.state = state;
        this.type = type;
        this.payload = payload;
        this.namespace = context.getNamespace((InternalPartitionedState<N>) state);
    }

    public StateRequestType getRequestType() {
        return type;
    }

    @Nullable
    public IN getPayload() {
        return payload;
    }

    @Nullable
    public State getState() {
        return state;
    }

    @Override
    @SuppressWarnings("unchecked")
    public InternalAsyncFuture<OUT> getFuture() {
        return asyncFuture;
    }

    public @Nonnull N getNamespace() {
        return namespace;
    }
}
