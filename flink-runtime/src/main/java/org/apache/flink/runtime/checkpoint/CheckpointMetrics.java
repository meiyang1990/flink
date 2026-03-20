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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A collection of simple metrics, around the triggering of a checkpoint.
 *
 * <p>【学习型注释】中文解释：该类用于收集与检查点触发相关的度量指标，
 * 包含了对齐阶段数据处理量、耗时、同步/异步执行时长等关键信息，用于监控检查点的执行效率。
 */
public class CheckpointMetrics implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final long UNSET = -1L;

    private final long bytesProcessedDuringAlignment;

    private final long bytesPersistedDuringAlignment;

    /** The duration (in nanoseconds) that the stream alignment for the checkpoint took. */
    private final long alignmentDurationNanos;

    /** The duration (in milliseconds) of the synchronous part of the operator checkpoint. */
    private final long syncDurationMillis;

    /** The duration (in milliseconds) of the asynchronous part of the operator checkpoint. */
    private final long asyncDurationMillis;

    private final long checkpointStartDelayNanos;

    /** Is the checkpoint completed as an unaligned checkpoint. */
    private final boolean unalignedCheckpoint;

    private final long bytesPersistedOfThisCheckpoint;

    private final long totalBytesPersisted;

    @VisibleForTesting
    public CheckpointMetrics() {
        this(UNSET, UNSET, UNSET, UNSET, UNSET, UNSET, false, 0L, 0L);
    }

    public CheckpointMetrics(
            long bytesProcessedDuringAlignment,
            long bytesPersistedDuringAlignment,
            long alignmentDurationNanos,
            long syncDurationMillis,
            long asyncDurationMillis,
            long checkpointStartDelayNanos,
            boolean unalignedCheckpoint,
            long bytesPersistedOfThisCheckpoint,
            long totalBytesPersisted) {

        // 校验输入参数合法性，支持传入 UNSET（-1）表示数值未知
        checkArgument(bytesProcessedDuringAlignment >= -1);
        checkArgument(bytesPersistedDuringAlignment >= -1);
        checkArgument(syncDurationMillis >= -1);
        checkArgument(asyncDurationMillis >= -1);
        checkArgument(alignmentDurationNanos >= -1);
        checkArgument(checkpointStartDelayNanos >= -1);
        checkArgument(bytesPersistedOfThisCheckpoint >= 0);
        checkArgument(totalBytesPersisted >= 0);

        this.bytesProcessedDuringAlignment = bytesProcessedDuringAlignment;
        this.bytesPersistedDuringAlignment = bytesPersistedDuringAlignment;
        this.alignmentDurationNanos = alignmentDurationNanos;
        this.syncDurationMillis = syncDurationMillis;
        this.asyncDurationMillis = asyncDurationMillis;
        this.checkpointStartDelayNanos = checkpointStartDelayNanos;
        this.unalignedCheckpoint = unalignedCheckpoint;
        this.bytesPersistedOfThisCheckpoint = bytesPersistedDuringAlignment;
        this.totalBytesPersisted = totalBytesPersisted;
    }

    public long getBytesProcessedDuringAlignment() {
        return bytesProcessedDuringAlignment;
    }

    public long getBytesPersistedDuringAlignment() {
        return bytesPersistedDuringAlignment;
    }

    public long getAlignmentDurationNanos() {
        return alignmentDurationNanos;
    }

    public long getSyncDurationMillis() {
        return syncDurationMillis;
    }

    public long getAsyncDurationMillis() {
        return asyncDurationMillis;
    }

    public long getCheckpointStartDelayNanos() {
        return checkpointStartDelayNanos;
    }

    public boolean getUnalignedCheckpoint() {
        return unalignedCheckpoint;
    }

    public long getBytesPersistedOfThisCheckpoint() {
        return bytesPersistedOfThisCheckpoint;
    }

    public long getTotalBytesPersisted() {
        return totalBytesPersisted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CheckpointMetrics that = (CheckpointMetrics) o;

        return bytesProcessedDuringAlignment == that.bytesProcessedDuringAlignment
                && bytesPersistedDuringAlignment == that.bytesPersistedDuringAlignment
                && alignmentDurationNanos == that.alignmentDurationNanos
                && syncDurationMillis == that.syncDurationMillis
                && asyncDurationMillis == that.asyncDurationMillis
                && checkpointStartDelayNanos == that.checkpointStartDelayNanos
                && unalignedCheckpoint == that.unalignedCheckpoint
                && bytesPersistedOfThisCheckpoint == that.bytesPersistedOfThisCheckpoint
                && totalBytesPersisted == that.totalBytesPersisted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bytesProcessedDuringAlignment,
                bytesPersistedDuringAlignment,
                alignmentDurationNanos,
                syncDurationMillis,
                asyncDurationMillis,
                checkpointStartDelayNanos,
                unalignedCheckpoint,
                totalBytesPersisted,
                bytesPersistedOfThisCheckpoint);
    }

    @Override
    public String toString() {
        return "CheckpointMetrics{"
                + "bytesProcessedDuringAlignment="
                + bytesProcessedDuringAlignment
                + ", bytesPersistedDuringAlignment="
                + bytesPersistedDuringAlignment
                + ", alignmentDurationNanos="
                + alignmentDurationNanos
                + ", syncDurationMillis="
                + syncDurationMillis
                + ", asyncDurationMillis="
                + asyncDurationMillis
                + ", checkpointStartDelayNanos="
                + checkpointStartDelayNanos
                + ", unalignedCheckpoint="
                + unalignedCheckpoint
                + ", bytesPersistedOfThisCheckpoint="
                + bytesPersistedOfThisCheckpoint
                + ", totalBytesPersisted="
                + totalBytesPersisted
                + '}';
    }
}
