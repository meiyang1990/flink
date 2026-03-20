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

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

/**
 * Exception returned to a TaskManager on JobMaster requesting partition state, if the producer of a
 * partition has been disposed.
 *
 * <p>【学习型注释】
 * 当 TaskManager 向 JobMaster 查询某个 ResultPartition 的状态时，
 * 如果该分区的生产者（Producer Task）已经被释放或取消，则抛出此异常。
 * 这通常发生在作业失败、取消或重新调度时，下游消费者尝试读取已失效的数据分区。
 */
public class PartitionProducerDisposedException extends Exception {

    public PartitionProducerDisposedException(ResultPartitionID resultPartitionID) {
        super(
                String.format(
                        "Execution %s producing partition %s has already been disposed.",
                        resultPartitionID.getProducerId(), resultPartitionID.getPartitionId()));
    }
}
