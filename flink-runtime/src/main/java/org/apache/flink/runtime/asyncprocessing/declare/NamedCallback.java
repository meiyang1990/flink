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

/** A named callback that can be identified and checkpoint. */
// 【学习型注释】回调函数的抽象基类，引入名称标识。
// 在 FLIP-425 的设计中，回调函数的持久化恢复依赖于其命名唯一性，以确保在不同检查点间匹配到正确的处理逻辑。
@Experimental
public abstract class NamedCallback {

    private String name;

    public NamedCallback(String name) {
        this.name = name;
    }

    /** Get the name of this callback. */
    public String getName() {
        return name;
    }
}
