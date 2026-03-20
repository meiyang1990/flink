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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemoryUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * JobManager utils to calculate {@link JobManagerProcessSpec} and JVM args.
 *
 * <p>【学习型注释】
 * JobManagerProcessUtils 提供了计算 JobManager 内存规格和 JVM 参数的工具方法。
 * 它根据用户配置（如总内存、堆内存等）计算出详细的内存分配方案，
 * 包括 JVM 堆内存、非堆内存、元空间和开销等，用于生成 JobManager 启动参数。
 */
public class JobManagerProcessUtils {

    static final ProcessMemoryOptions JM_PROCESS_MEMORY_OPTIONS =
            new ProcessMemoryOptions(
                    Collections.singletonList(JobManagerOptions.JVM_HEAP_MEMORY),
                    JobManagerOptions.TOTAL_FLINK_MEMORY,
                    JobManagerOptions.TOTAL_PROCESS_MEMORY,
                    new JvmMetaspaceAndOverheadOptions(
                            JobManagerOptions.JVM_METASPACE,
                            JobManagerOptions.JVM_OVERHEAD_MIN,
                            JobManagerOptions.JVM_OVERHEAD_MAX,
                            JobManagerOptions.JVM_OVERHEAD_FRACTION));

    private static final ProcessMemoryUtils<JobManagerFlinkMemory> PROCESS_MEMORY_UTILS =
            new ProcessMemoryUtils<>(JM_PROCESS_MEMORY_OPTIONS, new JobManagerFlinkMemoryUtils());

    private JobManagerProcessUtils() {}

    public static JobManagerProcessSpec processSpecFromConfig(Configuration config) {
        return createMemoryProcessSpec(PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config));
    }

    private static JobManagerProcessSpec createMemoryProcessSpec(
            CommonProcessMemorySpec<JobManagerFlinkMemory> processMemory) {
        return new JobManagerProcessSpec(
                processMemory.getFlinkMemory(), processMemory.getJvmMetaspaceAndOverhead());
    }

    @VisibleForTesting
    public static JobManagerProcessSpec createDefaultJobManagerProcessSpec(
            int totalProcessMemoryMb) {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.ofMebiBytes(totalProcessMemoryMb));
        return processSpecFromConfig(configuration);
    }

    public static String generateJvmParametersStr(
            JobManagerProcessSpec processSpec, Configuration configuration) {
        return ProcessMemoryUtils.generateJvmParametersStr(
                processSpec, configuration.get(JobManagerOptions.JVM_DIRECT_MEMORY_LIMIT_ENABLED));
    }

    public static String generateDynamicConfigsStr(
            final JobManagerProcessSpec jobManagerProcessSpec) {
        final Map<String, String> config = new HashMap<>();

        config.put(
                JobManagerOptions.JVM_HEAP_MEMORY.key(),
                jobManagerProcessSpec.getJvmHeapMemorySize().getBytes() + "b");
        config.put(
                JobManagerOptions.OFF_HEAP_MEMORY.key(),
                jobManagerProcessSpec.getJvmDirectMemorySize().getBytes() + "b");

        config.put(
                JobManagerOptions.JVM_METASPACE.key(),
                jobManagerProcessSpec.getJvmMetaspaceSize().getBytes() + "b");
        config.put(
                JobManagerOptions.JVM_OVERHEAD_MIN.key(),
                jobManagerProcessSpec.getJvmOverheadSize().getBytes() + "b");
        config.put(
                JobManagerOptions.JVM_OVERHEAD_MAX.key(),
                jobManagerProcessSpec.getJvmOverheadSize().getBytes() + "b");

        return ConfigurationUtils.assembleDynamicConfigsStr(config);
    }
}
