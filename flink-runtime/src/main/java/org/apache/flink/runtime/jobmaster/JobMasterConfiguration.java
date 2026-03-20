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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Configuration for the {@link JobMaster}.
 *
 * <p>【学习型注释】
 * JobMasterConfiguration 封装了 JobMaster 运行所需的配置参数。
 * 主要配置项包括：
 * - rpcTimeout: RPC 调用超时时间，影响与 ResourceManager、TaskExecutor 通信的容错性
 * - slotRequestTimeout: Slot 请求超时时间，决定申请资源的最大等待时间
 * - tmpDirectory: 临时目录路径，用于存储作业运行期间的临时文件
 * - retryingRegistrationConfiguration: 重试注册配置，控制向 ResourceManager 注册的重试策略
 * 这些配置直接影响 JobMaster 的稳定性、资源获取能力和故障恢复能力。
 */
public class JobMasterConfiguration {

    private final Duration rpcTimeout;

    private final Duration slotRequestTimeout;

    private final String tmpDirectory;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private final Configuration configuration;

    public JobMasterConfiguration(
            Duration rpcTimeout,
            Duration slotRequestTimeout,
            String tmpDirectory,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration,
            Configuration configuration) {
        this.rpcTimeout = Preconditions.checkNotNull(rpcTimeout);
        this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
        this.tmpDirectory = Preconditions.checkNotNull(tmpDirectory);
        this.retryingRegistrationConfiguration = retryingRegistrationConfiguration;
        this.configuration = Preconditions.checkNotNull(configuration);
    }

    public Duration getRpcTimeout() {
        return rpcTimeout;
    }

    public Duration getSlotRequestTimeout() {
        return slotRequestTimeout;
    }

    public String getTmpDirectory() {
        return tmpDirectory;
    }

    public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
        return retryingRegistrationConfiguration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static JobMasterConfiguration fromConfiguration(Configuration configuration) {

        final Duration rpcTimeout = configuration.get(RpcOptions.ASK_TIMEOUT_DURATION);

        final Duration slotRequestTimeout =
                configuration.get(JobManagerOptions.SLOT_REQUEST_TIMEOUT);

        final String tmpDirectory = ConfigurationUtils.parseTempDirectories(configuration)[0];

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);

        return new JobMasterConfiguration(
                rpcTimeout,
                slotRequestTimeout,
                tmpDirectory,
                retryingRegistrationConfiguration,
                configuration);
    }
}
