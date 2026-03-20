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

import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;

/** The Sync point request that will be trigger callback once it is allowed to be triggered. */
// 【学习型注释】同步点请求，用于在异步处理框架中实现 key 粒度的同步屏障。
// 当一个 key 正在被其他请求处理时，新的同步点请求会排队等待直到该 key 可用，
// 构造时 sync=true 表示这是一个同步请求。
public class SyncPointRequest<K> extends AsyncRequest<K> {

    public SyncPointRequest(RecordContext<K> context, InternalAsyncFuture<Void> stateFuture) {
        super(context, true, stateFuture);
    }
}
