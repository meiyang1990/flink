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
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.concurrent;

import java.util.concurrent.Executor;

/**
 * {@link Executor} implementation which fails when {@link #execute(Runnable)} is called. This can
 * be helpful if one wants to make sure that an executor is never been used.
 * 
 * <p>【学习型注释】一个不支持操作的 Executor 实现。
 * 当尝试调用 execute 时会抛出 UnsupportedOperationException。
 * 通常用于某些组件要求传入 Executor，但我们明确不希望该组件进行异步执行的场景。
 */
public enum UnsupportedOperationExecutor implements Executor {
    INSTANCE;

    @Override
    public void execute(Runnable command) {
        throw new UnsupportedOperationException("This executor should never been used.");
    }
}
