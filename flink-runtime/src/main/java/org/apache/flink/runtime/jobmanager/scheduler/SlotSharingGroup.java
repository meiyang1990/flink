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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot sharing units defines which different task (from different job vertices) can be deployed
 * together within a slot. This is a soft permission, in contrast to the hard constraint defined by
 * a co-location hint.
 *
 * <p>【学习型注释】
 * SlotSharingGroup 定义了哪些来自不同 JobVertex 的 Task 可以被部署在同一个 Slot 中执行。
 * 这是 Flink 提高资源利用率的核心机制之一：
 * - 例如：一个 Source 算子和一个 Map 算子可以被分配到同一个 Slot，减少跨节点网络传输。
 * - 与 CoLocationGroup 的硬约束不同，这是软约束，调度器会尽量满足但不是必须。
 * - 通过共享 Slot，可以在有限的 TaskManager 资源下运行更多的 Task。
 */
public class SlotSharingGroup implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<JobVertexID> ids = new TreeSet<>();

    private final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

    private String slotSharingGroupName;

    // Represents resources of all tasks in the group. Default to be UNKNOWN.
    private ResourceProfile resourceProfile = ResourceProfile.UNKNOWN;

    // --------------------------------------------------------------------------------------------

    public void addVertexToGroup(final JobVertexID id) {
        ids.add(checkNotNull(id));
    }

    public void removeVertexFromGroup(final JobVertexID id) {
        ids.remove(checkNotNull(id));
    }

    public Set<JobVertexID> getJobVertexIds() {
        return Collections.unmodifiableSet(ids);
    }

    public SlotSharingGroupId getSlotSharingGroupId() {
        return slotSharingGroupId;
    }

    public void setResourceProfile(ResourceProfile resourceProfile) {
        this.resourceProfile = checkNotNull(resourceProfile);
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public String getSlotSharingGroupName() {
        return slotSharingGroupName;
    }

    public void setSlotSharingGroupName(String slotSharingGroupName) {
        this.slotSharingGroupName = slotSharingGroupName;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(slotSharingGroupId);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotSharingGroup that = (SlotSharingGroup) o;
        return Objects.equals(slotSharingGroupId, that.slotSharingGroupId);
    }

    @Override
    public String toString() {
        return "SlotSharingGroup{"
                + "ids="
                + ids
                + ", slotSharingGroupId="
                + slotSharingGroupId
                + ", slotSharingGroupName='"
                + slotSharingGroupName
                + '\''
                + ", resourceProfile="
                + resourceProfile
                + '}';
    }
}
