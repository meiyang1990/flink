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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;

import java.util.List;

/**
 * {@code CoLocationGroup} refers to a list of {@link JobVertex} instances, where the <i>i-th</i>
 * subtask of one vertex has to be executed on the same {@code TaskManager} as the <i>i-th</i>
 * subtask of all other {@code JobVertex} instances in the same group.
 *
 * <p>The co-location group is used to make sure that the i-th subtasks for iteration head and
 * iteration tail are scheduled on the same TaskManager.
 *
 * <p>【学习型注释】
 * CoLocationGroup 是一种强约束机制，要求同一组 JobVertex 的第 i 个 subtask 必须运行在相同的 TaskManager 上。
 * 主要应用场景：
 * - 迭代计算：确保迭代头（iteration head）和迭代尾（iteration tail）在同一节点，避免跨网络的数据传输。
 * - 与 SlotSharingGroup 不同，这是硬约束，调度器必须满足，否则任务无法启动。
 */
public interface CoLocationGroup {

    /**
     * Returns the unique identifier describing this co-location constraint as a group.
     *
     * @return The group's identifier.
     */
    AbstractID getId();

    /**
     * Returns the IDs of the {@link JobVertex} instances participating in this group.
     *
     * @return The group's members represented by their {@link JobVertexID}s.
     */
    List<JobVertexID> getVertexIds();

    /**
     * Returns the {@link CoLocationConstraint} for a specific {@code subTaskIndex}.
     *
     * @param subTaskIndex The index of the subtasks for which a {@code CoLocationConstraint} shall
     *     be returned.
     * @return The corresponding {@code CoLocationConstraint} instance.
     */
    CoLocationConstraint getLocationConstraint(final int subTaskIndex);
}
