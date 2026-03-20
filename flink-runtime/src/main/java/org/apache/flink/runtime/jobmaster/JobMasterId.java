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

import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * The {@link JobMaster} fencing token.
 *
 * <p>【学习型注释】JobMasterId 是 JobMaster 的隔离令牌（Fencing Token），用于在分布式环境中唯一标识一个 JobMaster 实例。
 *
 * <p>核心作用：
 * 1. 领导权验证：当 JobManager 成为 Leader 时生成唯一 ID，用于验证 RPC 请求的合法性
 * 2. 防止脑裂：旧 Leader 的 RPC 请求因 ID 不匹配而被拒绝，避免 split-brain 问题
 * 3. 会话标识：关联 Leader 会话（leaderSessionId），标识一次领导任期
 *
 * <p>实现细节：
 * - 继承 AbstractID，基于 UUID 的 128 位唯一标识
 * - 提供 fromUuidOrNull 方法处理可能的 null 输入
 * - 可双向转换：UUID <-> JobMasterId
 *
 * <p>使用场景：所有从 JobMaster 发送给 TaskExecutor 的 RPC 请求都携带此 ID，
 * TaskExecutor 通过验证 ID 确认请求来自当前有效的 JobMaster。
 */
public class JobMasterId extends AbstractID {

    private static final long serialVersionUID = -933276753644003754L;

    /** Creates a JobMasterId that takes the bits from the given UUID. */
    public JobMasterId(UUID uuid) {
        super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    /** Generates a new random JobMasterId. */
    private JobMasterId() {
        super();
    }

    /** Creates a UUID with the bits from this JobMasterId. */
    public UUID toUUID() {
        return new UUID(getUpperPart(), getLowerPart());
    }

    /** Generates a new random JobMasterId. */
    public static JobMasterId generate() {
        return new JobMasterId();
    }

    /**
     * If the given uuid is null, this returns null, otherwise a JobMasterId that corresponds to the
     * UUID, via {@link #JobMasterId(UUID)}.
     */
    public static JobMasterId fromUuidOrNull(@Nullable UUID uuid) {
        return uuid == null ? null : new JobMasterId(uuid);
    }
}
