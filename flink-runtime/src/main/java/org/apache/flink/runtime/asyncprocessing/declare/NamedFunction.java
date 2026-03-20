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

package org.apache.flink.runtime.asyncprocessing.declare;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.function.FunctionWithException;

import java.util.function.Function;

/** A named version of {@link Function}. */
// 【学习型注释】命名的 Function 实现，支持异步处理中的转换逻辑具名化。
// 在状态处理回调链路构建中，通过名称标识回调函数，方便状态后端进行管理与序列化存储。
@Experimental
public class NamedFunction<T, R> extends NamedCallback
        implements FunctionWithException<T, R, Exception> {

    FunctionWithException<T, R, ? extends Exception> function;

    public NamedFunction(String name, FunctionWithException<T, R, ? extends Exception> function) {
        super(name);
        this.function = function;
    }

    @Override
    public R apply(T t) throws Exception {
        return function.apply(t);
    }
}
