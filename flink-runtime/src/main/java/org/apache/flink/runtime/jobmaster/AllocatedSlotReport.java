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

import org.apache.flink.api.common.JobID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The report of currently allocated slots from a given TaskExecutor by a JobMaster. This report is
 * sent periodically to the TaskExecutor in order to reconcile the internal state of slot
 * allocations.
 *
 * <p>【学习型注释】
 * AllocatedSlotReport 是 JobMaster 向 TaskExecutor 定期发送的 Slot 分配报告。
 * 用于部署一致性检查（Deployment Reconciliation），确保双方对 Slot 分配状态达成一致：
 * - JobMaster 定期发送该报告给 TaskExecutor
 * - TaskExecutor 对比报告中的 Slot 列表与本地实际部署的 Task
 * - 发现不一致时（如 JobMaster 认为某 Slot 已分配但 TaskExecutor 未部署），触发协调处理
 * 这种机制提高了系统的容错性，能够及时发现和修复状态不一致问题。
 */
public class AllocatedSlotReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final JobID jobId;

    /** The allocated slots in slot pool. */
    private final Collection<AllocatedSlotInfo> allocatedSlotInfos;

    public AllocatedSlotReport(JobID jobId, Collection<AllocatedSlotInfo> allocatedSlotInfos) {
        this.jobId = checkNotNull(jobId);
        this.allocatedSlotInfos = checkNotNull(allocatedSlotInfos);
    }

    public JobID getJobId() {
        return jobId;
    }

    public Collection<AllocatedSlotInfo> getAllocatedSlotInfos() {
        return Collections.unmodifiableCollection(allocatedSlotInfos);
    }

    @Override
    public String toString() {
        return "AllocatedSlotReport{"
                + "jobId="
                + jobId
                + ", allocatedSlotInfos="
                + allocatedSlotInfos
                + '}';
    }
}
