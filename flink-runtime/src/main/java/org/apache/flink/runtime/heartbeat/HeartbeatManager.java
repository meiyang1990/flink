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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

/**
 * A heartbeat manager has to be able to start/stop monitoring a {@link HeartbeatTarget}, and report
 * heartbeat timeouts for this target.
 *
 * <p>【学习笔记】HeartbeatManager 是 Flink 分布式系统中的心跳管理核心接口。
 *
 * <h3>一、设计目的</h3>
 * <ul>
 *   <li><b>故障检测</b>：通过心跳超时快速发现失联节点（TaskManager/ResourceManager/JobManager）</li>
 *   <li><b>状态同步</b>：心跳消息可携带 Payload，实现组件间的周期性状态交换</li>
 * </ul>
 *
 * <h3>二、核心交互模式</h3>
 * <p>Flink 采用双向心跳机制：
 * <ul>
 *   <li><b>主动发起方</b>（如 ResourceManager）：调用 requestHeartbeat() 请求对方发送心跳</li>
 *   <li><b>被动响应方</b>（如 TaskManager）：收到请求后调用 receiveHeartbeat() 回复心跳</li>
 * </ul>
 *
 * <h3>三、超时处理</h3>
 * <p>当某个监控目标在 heartbeatTimeout 时间内未发送心跳，HeartbeatListener 会收到超时通知，
 * 触发故障恢复流程（如 TaskManager 失联后重新调度 Task）。
 *
 * <h3>四、泛型参数</h3>
 * <ul>
 *   <li>{@code I}：接收的心跳负载类型（从监控目标收到的信息）</li>
 *   <li>{@code O}：发送的心跳负载类型（发给监控目标的信息）</li>
 * </ul>
 *
 * @param <I> Type of the incoming payload
 * @param <O> Type of the outgoing payload
 */
public interface HeartbeatManager<I, O> extends HeartbeatTarget<I> {

    /**
     * Start monitoring a {@link HeartbeatTarget}. Heartbeat timeouts for this target are reported
     * to the {@link HeartbeatListener} associated with this heartbeat manager.
     *
     * @param resourceID Resource ID identifying the heartbeat target
     * @param heartbeatTarget Interface to send heartbeat requests and responses to the heartbeat
     *     target
     */
    void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget);

    /**
     * Stops monitoring the heartbeat target with the associated resource ID.
     *
     * @param resourceID Resource ID of the heartbeat target which shall no longer be monitored
     */
    void unmonitorTarget(ResourceID resourceID);

    /** Stops the heartbeat manager. */
    void stop();

    /**
     * Returns the last received heartbeat from the given target.
     *
     * @param resourceId for which to return the last heartbeat
     * @return Last heartbeat received from the given target or -1 if the target is not being
     *     monitored.
     */
    long getLastHeartbeatFrom(ResourceID resourceId);
}
