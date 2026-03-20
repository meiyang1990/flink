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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OptionalFailure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;

/** Container class that transports the result of an accumulator as set of strings. */
// 【学习型注释】累加器结果的字符串化表示，用于 REST API 和 Client 端展示。
// 将累加器的名称、类型和值统一转为 String，以便通过 JSON 等文本协议传输给用户。
// 支持处理累加器计算失败的情况：失败时 value 字段包含异常堆栈信息。
public class StringifiedAccumulatorResult implements java.io.Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(StringifiedAccumulatorResult.class);

    private static final long serialVersionUID = -4642311296836822611L;

    private final String name;
    private final String type;
    private final String value;

    public StringifiedAccumulatorResult(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Flatten a map of accumulator names to Accumulator instances into an array of
     * StringifiedAccumulatorResult values.
     */
    // 【学习型注释】将累加器 Map 批量转换为字符串数组，供 REST API 或 Client 消费。
    // 使用 OptionalFailure 包装以区分正常值和异常情况
    public static StringifiedAccumulatorResult[] stringifyAccumulatorResults(
            Map<String, OptionalFailure<Accumulator<?, ?>>> accs) {
        if (accs == null || accs.isEmpty()) {
            return new StringifiedAccumulatorResult[0];
        } else {
            StringifiedAccumulatorResult[] results = new StringifiedAccumulatorResult[accs.size()];

            int i = 0;
            for (Map.Entry<String, OptionalFailure<Accumulator<?, ?>>> entry : accs.entrySet()) {
                results[i++] = stringifyAccumulatorResult(entry.getKey(), entry.getValue());
            }
            return results;
        }
    }

    // 【学习型注释】处理单个累加器的三种状态：null、失败、正常取值
    private static StringifiedAccumulatorResult stringifyAccumulatorResult(
            String name, @Nullable OptionalFailure<Accumulator<?, ?>> accumulator) {
        if (accumulator == null) {
            return new StringifiedAccumulatorResult(name, "null", "null");
        } else if (accumulator.isFailure()) {
            // 【学习型注释】累加器计算失败时，将异常信息作为 value 展示
            return new StringifiedAccumulatorResult(
                    name, "null", ExceptionUtils.stringifyException(accumulator.getFailureCause()));
        } else {
            Object localValue;
            String simpleName = "null";
            try {
                simpleName = accumulator.getUnchecked().getClass().getSimpleName();
                localValue = accumulator.getUnchecked().getLocalValue();
            } catch (RuntimeException exception) {
                LOG.error("Failed to stringify accumulator [" + name + "]", exception);
                localValue = ExceptionUtils.stringifyException(exception);
            }
            return new StringifiedAccumulatorResult(
                    name, simpleName, localValue != null ? localValue.toString() : "null");
        }
    }
}
