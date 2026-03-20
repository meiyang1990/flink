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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.AbstractID;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code CoLocationConstraint} stores the ID of {@link CoLocationGroup} and an ID referring to
 * the actual subtask (i.e. {@link ExecutionVertex}). In co-location groups, the different subtasks
 * of different {@link JobVertex} instances need to be executed on the same slot. This is realized
 * by creating a special shared slot that holds these tasks.
 *
 * <p>【学习型注释】
 * CoLocationConstraint 是 CoLocationGroup 的具体约束实现。
 * 它通过 coLocationGroupId 标识所属的约束组，通过 constraintIndex 标识组内的第几个 subtask。
 * 调度器使用这个约束来确保同一索引位置的不同 JobVertex 的 subtask 被分配到同一个 Slot。
 */
public class CoLocationConstraint {

    private final AbstractID coLocationGroupId;

    private final int constraintIndex;

    CoLocationConstraint(final AbstractID coLocationGroupId, final int constraintIndex) {
        this.coLocationGroupId = checkNotNull(coLocationGroupId);
        this.constraintIndex = constraintIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            CoLocationConstraint that = (CoLocationConstraint) obj;
            return Objects.equals(that.coLocationGroupId, this.coLocationGroupId)
                    && that.constraintIndex == this.constraintIndex;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * coLocationGroupId.hashCode() + constraintIndex;
    }
}
