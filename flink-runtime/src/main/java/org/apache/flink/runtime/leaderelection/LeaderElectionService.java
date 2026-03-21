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

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;

/**
 * Interface for a service which allows to elect a leader among a group of contenders.
 *
 * <p>Prior to using this service, it has to be started calling the start method. The start method
 * takes the contender as a parameter. If there are multiple contenders, then each contender has to
 * instantiate its own leader election service.
 *
 * <p>Once a contender has been granted leadership he has to confirm the received leader session ID
 * by calling the method {@link LeaderElection#confirmLeadershipAsync(UUID, String)}. This will
 * notify the leader election service, that the contender has accepted the leadership specified and
 * that the leader session id as well as the leader address can now be published for leader
 * retrieval services.
 *
 * <p>【学习笔记】LeaderElectionService 是 Flink 高可用架构的核心接口，负责 Leader 选举。
 *
 * <h3>一、设计目的</h3>
 * <p>在分布式环境中，同一时刻只能有一个节点作为 Leader 执行关键任务（如 JobManager 调度作业）。
 * 本接口抽象了 Leader 选举逻辑，使得 Flink 可以支持多种 HA 后端。
 *
 * <h3>二、支持的 HA 后端</h3>
 * <ul>
 *   <li><b>ZooKeeper</b>：基于临时顺序节点实现分布式锁和选举</li>
 *   <li><b>Kubernetes</b>：基于 ConfigMap 和 Lease 实现选举</li>
 * </ul>
 *
 * <h3>三、选举流程</h3>
 * <ol>
 *   <li><b>注册竞选者</b>：通过 createLeaderElection() 创建选举实例</li>
 *   <li><b>等待选举结果</b>：HA 后端通知当前节点成为 Leader</li>
 *   <li><b>确认 Leadership</b>：调用 confirmLeadershipAsync() 确认接受领导权</li>
 *   <li><b>发布 Leader 信息</b>：Leader 地址和 Session ID 写入 HA 存储，供其他组件发现</li>
 * </ol>
 *
 * <h3>四、Leader Session ID</h3>
 * <p>每次选举产生唯一的 UUID，用于：
 * <ul>
 *   <li>检测旧 Leader 的消息（Session ID 不匹配时拒绝）</li>
 *   <li>防止脑裂（Split-Brain）问题</li>
 * </ul>
 */
public interface LeaderElectionService {

    /**
     * Creates a new {@link LeaderElection} instance that is registered to this {@code
     * LeaderElectionService} instance.
     *
     * @param componentId a unique identifier that refers to the stored leader information that the
     *     newly created {@link LeaderElection} manages.
     */
    LeaderElection createLeaderElection(String componentId);
}
