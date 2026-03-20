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

package org.apache.flink.runtime.checkpoint;

/** 
 * Various reasons why a checkpoint was failure. 
 * 
 * <p>【学习型注释】中文解释：枚举了导致检查点失败的各种原因。区分了“预飞行（Pre-flight）”阶段失败和
 * 执行过程中失败，这有助于决策是否进行容错恢复（Failover）或重试。
 */
public enum CheckpointFailureReason {
    /** 周期性检查点调度器已关闭 */
    PERIODIC_SCHEDULER_SHUTDOWN(true, "Periodic checkpoint scheduler is shut down."),

    /** 检查点队列请求过多 */
    TOO_MANY_CHECKPOINT_REQUESTS(true, "The maximum number of queued checkpoint requests exceeded"),

    /** 检查点间隔时间未到 */
    MINIMUM_TIME_BETWEEN_CHECKPOINTS(
            true,
            "The minimum time between checkpoints is still pending. "
                    + "Checkpoint will be triggered after the minimum time."),

    /** 并非所有必要的任务都在运行 */
    NOT_ALL_REQUIRED_TASKS_RUNNING(true, "Not all required tasks are currently running."),

    /** 触发检查点时发生的 IO 异常 */
    IO_EXCEPTION(
            true, "An Exception occurred while triggering the checkpoint. IO-problem detected."),

    /** 存在阻塞输出边 */
    BLOCKING_OUTPUT_EXIST(true, "Blocking output edge exists in running tasks."),

    /** 异步检查点失败 */
    CHECKPOINT_ASYNC_EXCEPTION(false, "Asynchronous task checkpoint failed."),

    /** 通道状态共享流异常 */
    CHANNEL_STATE_SHARED_STREAM_EXCEPTION(
            false,
            "The checkpoint was aborted due to exception of other subtasks sharing the ChannelState file."),

    /** 检查点在完成前过期 */
    CHECKPOINT_EXPIRED(false, "Checkpoint expired before completing."),

    /** 检查点被更新的检查点取代 */
    CHECKPOINT_SUBSUMED(false, "Checkpoint has been subsumed."),

    /** 检查点被拒绝 */
    CHECKPOINT_DECLINED(false, "Checkpoint was declined."),

    /** 检查点被拒绝：任务未就绪 */
    CHECKPOINT_DECLINED_TASK_NOT_READY(false, "Checkpoint was declined (tasks not ready)"),

    /** 检查点被拒绝：任务正在关闭 */
    CHECKPOINT_DECLINED_TASK_CLOSING(false, "Checkpoint was declined (task is closing)"),

    /** 检查点被拒绝：收到较新检查点的屏障 */
    CHECKPOINT_DECLINED_SUBSUMED(
            false, "Checkpoint was canceled because a barrier from newer checkpoint was received."),

    /** 检查点被拒绝：收到取消屏障 */
    CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER(
            false, "Task received cancellation from one of its inputs"),

    /** 检查点被拒绝：输入流结束 */
    CHECKPOINT_DECLINED_INPUT_END_OF_STREAM(
            false, "Checkpoint was declined because one input stream is finished"),

    /** 协调器关闭 */
    CHECKPOINT_COORDINATOR_SHUTDOWN(false, "CheckpointCoordinator shutdown."),

    /** 协调器挂起 */
    CHECKPOINT_COORDINATOR_SUSPEND(false, "Checkpoint Coordinator is suspending."),

    /** 作业故障恢复区域重启 */
    JOB_FAILOVER_REGION(false, "FailoverRegion is restarting."),

    /** 任务失败 */
    TASK_FAILURE(false, "Task has failed."),

    /** 任务本地检查点失败 */
    TASK_CHECKPOINT_FAILURE(false, "Task local checkpoint failure."),

    /** 检查点通知的未知任务失败 */
    UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE(
            false, "Unknown task for the checkpoint to notify."),

    /** 最终化检查点失败 */
    FINALIZE_CHECKPOINT_FAILURE(false, "Failure to finalize checkpoint."),

    /** 触发检查点失败 */
    TRIGGER_CHECKPOINT_FAILURE(false, "Trigger checkpoint failure.");

    // ------------------------------------------------------------------------

    private final boolean preFlight;
    private final String message;

    CheckpointFailureReason(boolean isPreFlight, String message) {
        this.preFlight = isPreFlight;
        this.message = message;
    }

    public String message() {
        return message;
    }

    /**
     * @return true if this value indicates a failure reason happening before a checkpoint is passed
     *     to a job's tasks.
     */
    public boolean isPreFlight() {
        return preFlight;
    }
}
