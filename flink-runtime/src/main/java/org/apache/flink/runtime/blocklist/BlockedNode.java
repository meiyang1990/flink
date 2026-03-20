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

// 这个文件已经全部加上中文注释

package org.apache.flink.runtime.blocklist;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents a blocked node record.
 *
 * <p>【学习型注释】
 * BlockedNode 表示一个被屏蔽的节点记录，用于故障节点管理。
 *
 * <p>字段说明：
 * - nodeId: 节点唯一标识（通常是主机名或IP）
 * - cause: 被屏蔽的原因（如频繁Task失败、心跳丢失等）
 * - endTimestamp: 屏蔽结束时间戳（超时后自动解除屏蔽）
 *
 * <p>使用场景：
 * 当某个物理节点频繁出现Task失败时，系统会将其加入blocklist，
 * 避免新的Task被调度到该节点，直到屏蔽超时或管理员手动解除。
 */
public class BlockedNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final String cause;

    private final long endTimestamp;

    public BlockedNode(String nodeId, String cause, long endTimestamp) {
        this.nodeId = checkNotNull(nodeId);
        this.cause = checkNotNull(cause);
        this.endTimestamp = endTimestamp;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getCause() {
        return cause;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof BlockedNode) {
            BlockedNode other = (BlockedNode) obj;
            return nodeId.equals(other.nodeId)
                    && cause.equals(other.cause)
                    && endTimestamp == other.endTimestamp;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "BlockedNode{"
                + "id:"
                + nodeId
                + ",cause:"
                + cause
                + ",endTimestamp:"
                + endTimestamp
                + "}";
    }
}
