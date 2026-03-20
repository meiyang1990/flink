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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Helper class used to track and manage the relationships between shuffle descriptors and their
 * associated subpartitions.
 * <p>【学习型注释】用于追踪和管理 ShuffleDescriptor 与其对应子分区（Subpartition）关系的上下文工具类。
 * 核心逻辑在于将 ShuffleDescriptor 的索引范围映射到任务消费的子分区范围。
 */
class ConsumedSubpartitionContext implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The number of consumed shuffle descriptors. */
    private final int numConsumedShuffleDescriptors;

    /**
     * A mapping between ranges of consumed shuffle descriptors and their corresponding subpartition
     * ranges.
     * <p>【学习型注释】记录已消费 ShuffleDescriptor 的范围与子分区范围的映射。
     */
    private final Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap;

    private ConsumedSubpartitionContext(
            int numConsumedShuffleDescriptors,
            Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap) {
        this.numConsumedShuffleDescriptors = numConsumedShuffleDescriptors;
        this.consumedShuffleDescriptorToSubpartitionRangeMap =
                checkNotNull(consumedShuffleDescriptorToSubpartitionRangeMap);
    }

    public int getNumConsumedShuffleDescriptors() {
        return numConsumedShuffleDescriptors;
    }

    public Collection<IndexRange> getConsumedShuffleDescriptorRanges() {
        // The original consumed shuffle descriptors may have overlaps, we need to deduplicate it
        // by merging.
        return Collections.unmodifiableCollection(
                mergeIndexRanges(consumedShuffleDescriptorToSubpartitionRangeMap.keySet()));
    }

    /**
     * 【学习型注释】根据 ShuffleDescriptor 的索引查找对应的子分区范围。
     */
    public IndexRange getConsumedSubpartitionRange(int shuffleDescriptorIndex) {
        List<IndexRange> consumedSubpartitionRanges = new ArrayList<>();
        for (Map.Entry<IndexRange, IndexRange> entry :
                consumedShuffleDescriptorToSubpartitionRangeMap.entrySet()) {
            IndexRange shuffleDescriptorRange = entry.getKey();
            if (shuffleDescriptorIndex >= shuffleDescriptorRange.getStartIndex()
                    && shuffleDescriptorIndex <= shuffleDescriptorRange.getEndIndex()) {
                consumedSubpartitionRanges.add(entry.getValue());
            }
        }
        List<IndexRange> mergedConsumedSubpartitionRanges =
                mergeIndexRanges(consumedSubpartitionRanges);
        checkState(
                mergedConsumedSubpartitionRanges.size() == 1,
                "Illegal consumed subpartition range for shuffle descriptor index "
                        + shuffleDescriptorIndex);
        return mergedConsumedSubpartitionRanges.get(0);
    }

    /**
     * Builds a {@link ConsumedSubpartitionContext} based on the provided inputs.
     *
     * <p>Note: The construction is based on subscribing to consecutive subpartitions of the same
     * partition. If this assumption is violated, an exception will be thrown.
     * <p>【学习型注释】工厂方法：根据分区组、子分区组映射以及ID获取逻辑，构建上下文对象。
     */
    public static ConsumedSubpartitionContext buildConsumedSubpartitionContext(
            Map<IndexRange, IndexRange> consumedSubpartitionGroups,
            ConsumedPartitionGroup consumedPartitionGroup,
            Function<Integer, IntermediateResultPartitionID> partitionIdRetriever) {
        Map<IntermediateResultPartitionID, Integer> resultPartitionsInOrder =
                consumedPartitionGroup.getResultPartitionsInOrder();
        
        // 优化处理：如果是一对一映射，直接返回构建结果
        if (consumedSubpartitionGroups.size() == 1
                && consumedSubpartitionGroups.keySet().iterator().next().size()
                        == resultPartitionsInOrder.size()) {
            return buildConsumedSubpartitionContext(
                    resultPartitionsInOrder.size(),
                    consumedSubpartitionGroups.values().iterator().next());
        }

        Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap =
                new LinkedHashMap<>();
        for (Map.Entry<IndexRange, IndexRange> entry : consumedSubpartitionGroups.entrySet()) {
            IndexRange partitionRange = entry.getKey();
            IndexRange subpartitionRange = entry.getValue();
            // ShuffleDescriptor 索引与 resultPartitionsInOrder 顺序一致
            IndexRange shuffleDescriptorRange =
                    new IndexRange(
                            resultPartitionsInOrder.get(
                                    partitionIdRetriever.apply(partitionRange.getStartIndex())),
                            resultPartitionsInOrder.get(
                                    partitionIdRetriever.apply(partitionRange.getEndIndex())));
            checkState(
                    partitionRange.size() == shuffleDescriptorRange.size()
                            && !consumedShuffleDescriptorToSubpartitionRangeMap.containsKey(
                                    shuffleDescriptorRange));
            consumedShuffleDescriptorToSubpartitionRangeMap.put(
                    shuffleDescriptorRange, subpartitionRange);
        }
        
        // 统计合并后的 ShuffleDescriptor 数量
        int numConsumedShuffleDescriptors = 0;
        List<IndexRange> mergedConsumedShuffleDescriptor =
                mergeIndexRanges(consumedShuffleDescriptorToSubpartitionRangeMap.keySet());
        for (IndexRange range : mergedConsumedShuffleDescriptor) {
            numConsumedShuffleDescriptors += range.size();
        }
        return new ConsumedSubpartitionContext(
                numConsumedShuffleDescriptors, consumedShuffleDescriptorToSubpartitionRangeMap);
    }

    /**
     * Builds a {@link ConsumedSubpartitionContext} using a given number of consumed shuffle
     * descriptors and a single {@link IndexRange} representing the consumed subpartition range.
     *
     * <p>Note: This method is designed as a compatibility method. It assumes that the task will
     * subscribe to all shuffle descriptors and to the same subpartitions for every descriptor.
     */
    public static ConsumedSubpartitionContext buildConsumedSubpartitionContext(
            int numConsumedShuffleDescriptors, IndexRange consumedSubpartitionRange) {
        checkState(numConsumedShuffleDescriptors > 0);
        return new ConsumedSubpartitionContext(
                numConsumedShuffleDescriptors,
                Map.of(
                        new IndexRange(0, numConsumedShuffleDescriptors - 1),
                        consumedSubpartitionRange));
    }

    @Override
    public String toString() {
        return String.format(
                "ConsumedSubpartitionContext [num consumed shuffle descriptors: %s, "
                        + "consumed shuffle descriptors to subpartition range: %s]",
                numConsumedShuffleDescriptors, consumedShuffleDescriptorToSubpartitionRangeMap);
    }
}
