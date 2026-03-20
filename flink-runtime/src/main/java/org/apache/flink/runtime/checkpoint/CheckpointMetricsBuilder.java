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

import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.checkStateAndGet;

/**
 * A builder for {@link CheckpointMetrics}.
 *
 * <p>This class is not thread safe, but parts of it can actually be used from different threads.
 * 
 * <p>【学习型注释】中文解释：{@link CheckpointMetrics} 的构建器。用于在快照执行过程中逐步累积并设置检查点执行的指标数据，
 * 如对齐字节数、对齐耗时、同步/异步执行阶段耗时等。
 */
@NotThreadSafe
public class CheckpointMetricsBuilder {
    private CompletableFuture<Long> bytesProcessedDuringAlignment = new CompletableFuture<>();
    private long bytesPersistedDuringAlignment = -1L;
    private CompletableFuture<Long> alignmentDurationNanos = new CompletableFuture<>();
    private long syncDurationMillis = -1L;
    private long asyncDurationMillis = -1L;
    private long checkpointStartDelayNanos = -1L;
    private long totalBytesPersisted = -1L;
    private long bytesPersistedOfThisCheckpoint = -1L;

    public CheckpointMetricsBuilder setBytesProcessedDuringAlignment(
            long bytesProcessedDuringAlignment) {
        checkState(
                this.bytesProcessedDuringAlignment.complete(bytesProcessedDuringAlignment),
                "bytesProcessedDuringAlignment has already been completed by someone else");
        return this;
    }

    public CheckpointMetricsBuilder setBytesProcessedDuringAlignment(
            CompletableFuture<Long> bytesProcessedDuringAlignment) {
        this.bytesProcessedDuringAlignment = bytesProcessedDuringAlignment;
        return this;
    }

    public CompletableFuture<Long> getBytesProcessedDuringAlignment() {
        return bytesProcessedDuringAlignment;
    }

    public CheckpointMetricsBuilder setBytesPersistedDuringAlignment(
            long bytesPersistedDuringAlignment) {
        this.bytesPersistedDuringAlignment = bytesPersistedDuringAlignment;
        return this;
    }

    public long getAlignmentDurationNanosOrDefault() {
        return FutureUtils.getOrDefault(alignmentDurationNanos, -1L);
    }

    public CheckpointMetricsBuilder setAlignmentDurationNanos(long alignmentDurationNanos) {
        checkState(
                this.alignmentDurationNanos.complete(alignmentDurationNanos),
                "alignmentDurationNanos has already been completed by someone else");
        return this;
    }

    public CheckpointMetricsBuilder setAlignmentDurationNanos(
            CompletableFuture<Long> alignmentDurationNanos) {
        checkState(
                !this.alignmentDurationNanos.isDone(),
                "alignmentDurationNanos has already been completed by someone else");
        this.alignmentDurationNanos = alignmentDurationNanos;
        return this;
    }

    public CompletableFuture<Long> getAlignmentDurationNanos() {
        return alignmentDurationNanos;
    }

    public CheckpointMetricsBuilder setSyncDurationMillis(long syncDurationMillis) {
        this.syncDurationMillis = syncDurationMillis;
        return this;
    }

    public long getSyncDurationMillis() {
        return syncDurationMillis;
    }

    public CheckpointMetricsBuilder setAsyncDurationMillis(long asyncDurationMillis) {
        this.asyncDurationMillis = asyncDurationMillis;
        return this;
    }

    public long getAsyncDurationMillis() {
        return asyncDurationMillis;
    }

    public CheckpointMetricsBuilder setCheckpointStartDelayNanos(long checkpointStartDelayNanos) {
        this.checkpointStartDelayNanos = checkpointStartDelayNanos;
        return this;
    }

    public long getCheckpointStartDelayNanos() {
        return checkpointStartDelayNanos;
    }

    public CheckpointMetricsBuilder setTotalBytesPersisted(long totalBytesPersisted) {
        this.totalBytesPersisted = totalBytesPersisted;
        return this;
    }

    public long getBytesPersistedOfThisCheckpoint() {
        return bytesPersistedOfThisCheckpoint;
    }

    public CheckpointMetricsBuilder setBytesPersistedOfThisCheckpoint(
            long bytesPersistedOfThisCheckpoint) {
        this.bytesPersistedOfThisCheckpoint = bytesPersistedOfThisCheckpoint;
        return this;
    }

    /** 构建最终的检查点指标对象 */
    public CheckpointMetrics build() {
        return new CheckpointMetrics(
                checkStateAndGet(bytesProcessedDuringAlignment),
                bytesPersistedDuringAlignment,
                checkStateAndGet(alignmentDurationNanos),
                syncDurationMillis,
                asyncDurationMillis,
                checkpointStartDelayNanos,
                bytesPersistedDuringAlignment > 0,
                bytesPersistedOfThisCheckpoint,
                totalBytesPersisted);
    }

    /** 用于构建不完整的指标快照 */
    public CheckpointMetrics buildIncomplete() {
        return new CheckpointMetrics(
                bytesProcessedDuringAlignment.getNow(CheckpointMetrics.UNSET),
                bytesPersistedDuringAlignment,
                alignmentDurationNanos.getNow(CheckpointMetrics.UNSET),
                syncDurationMillis,
                asyncDurationMillis,
                checkpointStartDelayNanos,
                bytesPersistedDuringAlignment > 0,
                bytesPersistedOfThisCheckpoint,
                totalBytesPersisted);
    }
}
