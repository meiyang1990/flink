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
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.function.Consumer;

/** A named version of {@link Consumer}. */
// 【学习型注释】命名的 Consumer 实现，封装了可抛出异常的 ThrowingConsumer。
// 适配器模式应用，确保算子异步处理中的消费逻辑可以进行唯一命名与持久化序列化。
@Experimental
public class NamedConsumer<T> extends NamedCallback implements ThrowingConsumer<T, Exception> {

    ThrowingConsumer<? super T, ? extends Exception> consumer;

    public NamedConsumer(String name, ThrowingConsumer<? super T, ? extends Exception> consumer) {
        super(name);
        this.consumer = consumer;
    }

    public void accept(T t) throws Exception {
        consumer.accept(t);
    }
}
