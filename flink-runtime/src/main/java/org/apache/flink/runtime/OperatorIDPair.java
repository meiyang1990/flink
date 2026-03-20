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

package org.apache.flink.runtime;

import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** Formed of a mandatory operator ID and optionally a user defined operator ID. */
// 【学习型注释】操作符 ID 对，用于同时持有系统自动生成的 OperatorID 和用户自定义的 OperatorID。
// 在 Flink 的状态恢复和算子匹配中，用户自定义 ID 优先于生成 ID 使用，
// 这是为了支持作业拓扑变更后仍能正确恢复状态而设计的双重标识机制。
public class OperatorIDPair implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OperatorID generatedOperatorID;
    @Nullable private final OperatorID userDefinedOperatorID;
    @Nullable private final String userDefinedOperatorName;
    @Nullable private final String userDefinedOperatorUid;

    // 【学习型注释】私有构造函数，禁止外部直接实例化，必须通过工厂方法创建
    private OperatorIDPair(
            OperatorID generatedOperatorID,
            @Nullable OperatorID userDefinedOperatorID,
            @Nullable String userDefinedOperatorName,
            @Nullable String userDefinedOperatorUid) {
        this.generatedOperatorID = generatedOperatorID;
        this.userDefinedOperatorID = userDefinedOperatorID;
        // 【学习型注释】空字符串和 null 语义不同：null 表示未设置，空字符串非法
        if (userDefinedOperatorName != null && userDefinedOperatorName.isEmpty()) {
            throw new IllegalArgumentException("Empty string operator name is not allowed");
        }
        this.userDefinedOperatorName = userDefinedOperatorName;
        if (userDefinedOperatorUid != null && userDefinedOperatorUid.isEmpty()) {
            throw new IllegalArgumentException("Empty string operator uid is not allowed");
        }
        this.userDefinedOperatorUid = userDefinedOperatorUid;
    }

    public static OperatorIDPair of(
            OperatorID generatedOperatorID,
            @Nullable OperatorID userDefinedOperatorID,
            @Nullable String operatorName,
            @Nullable String operatorUid) {
        return new OperatorIDPair(
                generatedOperatorID, userDefinedOperatorID, operatorName, operatorUid);
    }

    public static OperatorIDPair generatedIDOnly(OperatorID generatedOperatorID) {
        return new OperatorIDPair(generatedOperatorID, null, null, null);
    }

    public OperatorID getGeneratedOperatorID() {
        return generatedOperatorID;
    }

    @Nullable
    public Optional<OperatorID> getUserDefinedOperatorID() {
        return Optional.ofNullable(userDefinedOperatorID);
    }

    @Nullable
    public String getUserDefinedOperatorName() {
        return userDefinedOperatorName;
    }

    @Nullable
    public String getUserDefinedOperatorUid() {
        return userDefinedOperatorUid;
    }
}
