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

// 这个文件已经全部加上中文注释

package org.apache.flink.runtime.blocklist;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlowTaskDetectorOptions;

/**
 * Utility class for blocklist.
 *
 * <p>【学习型注释】
 * BlocklistUtils 是屏蔽列表功能的工具类，提供工厂方法和配置检查。
 *
 * <p>核心功能：
 * 1. 根据配置加载合适的 BlocklistHandler 工厂
 * 2. 检查屏蔽功能是否启用
 *
 * <p>启用条件：
 * 目前屏蔽列表功能仅用于推测执行（Speculative Execution）场景，
 * 当启用推测执行时，慢任务检测器会将慢节点加入屏蔽列表。
 */
public class BlocklistUtils {

    /**
     * 根据 configuration 加载对应的 BlocklistHandler 工厂。
     * 如果屏蔽功能启用，返回 DefaultBlocklistHandler.Factory；
     * 否则返回 NoOpBlocklistHandler.Factory（空实现）。
     */
    public static BlocklistHandler.Factory loadBlocklistHandlerFactory(
            Configuration configuration) {
        if (isBlocklistEnabled(configuration)) {
            return new DefaultBlocklistHandler.Factory(
                    configuration.get(SlowTaskDetectorOptions.CHECK_INTERVAL));
        } else {
            return new NoOpBlocklistHandler.Factory();
        }
    }

    /**
     * 检查屏蔽功能是否启用。
     * 目前仅在推测执行启用时才启用屏蔽功能。
     */
    public static boolean isBlocklistEnabled(Configuration configuration) {
        // Currently, only enable blocklist for speculative execution
        return configuration.get(BatchExecutionOptions.SPECULATIVE_ENABLED);
    }

    /** Private default constructor to avoid being instantiated. */
    private BlocklistUtils() {}
}
