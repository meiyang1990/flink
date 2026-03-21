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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AckAllUserRecordsProcessed;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.NewBufferSize;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.netty.NettyMessage.SegmentId;
import org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 【学习笔记】PartitionRequestServerHandler - Netty 服务端消息处理器
 *
 * <p>核心职责：
 * 1. 处理来自下游 TaskManager 的分区请求（PartitionRequest），创建数据读取视图
 * 2. 接收并转发上游 Task 产生的 TaskEvent 事件
 * 3. 处理 Credit-based 流量控制相关消息（AddCredit、ResumeConsumption 等）
 *
 * <p>消息类型：
 * - PartitionRequest：请求指定分区的数据，触发 CreditBasedSequenceNumberingViewReader 创建
 * - TaskEventRequest：转发 Barrier、Watermark 等事件到上游
 * - AddCredit：下游通告可用 Credit，用于背压控制
 * - ResumeConsumption：Checkpoint 对齐后恢复数据消费
 * - CancelPartitionRequest：取消分区请求
 *
 * <p>数据流向：
 * RemoteInputChannel → NettyClient → [NettyServer → PartitionRequestServerHandler]
 *                    → PartitionRequestQueue → ResultSubpartition
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

    // 分区数据提供者，用于创建 SubpartitionView
    private final ResultPartitionProvider partitionProvider;

    // Task 事件发布器，用于向上游 Task 转发事件
    private final TaskEventPublisher taskEventPublisher;

    // 出站消息队列，负责实际的数据发送
    private final PartitionRequestQueue outboundQueue;

    PartitionRequestServerHandler(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            PartitionRequestQueue outboundQueue) {

        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    // 处理入站消息的核心方法
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();

            // ----------------------------------------------------------------
            // 分区数据请求：创建读取器并注册到出站队列
            // ----------------------------------------------------------------
            if (msgClazz == PartitionRequest.class) {
                PartitionRequest request = (PartitionRequest) msg;

                LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

                // 创建基于 Credit 的序列号读取器
                NetworkSequenceViewReader reader;
                reader =
                        new CreditBasedSequenceNumberingViewReader(
                                request.receiverId, request.credit, outboundQueue);

                // 请求 Subpartition 视图或注册监听器等待分区就绪
                reader.requestSubpartitionViewOrRegisterListener(
                        partitionProvider, request.partitionId, request.queueIndexSet);

            }
            // ----------------------------------------------------------------
            // Task 事件：转发到上游 Task（如 Barrier、Watermark）
            // ----------------------------------------------------------------
            else if (msgClazz == TaskEventRequest.class) {
                TaskEventRequest request = (TaskEventRequest) msg;

                if (!taskEventPublisher.publish(request.partitionId, request.event)) {
                    respondWithError(
                            ctx,
                            new IllegalArgumentException("Task event receiver not found."),
                            request.receiverId);
                }
            }
            // 取消分区请求
            else if (msgClazz == CancelPartitionRequest.class) {
                CancelPartitionRequest request = (CancelPartitionRequest) msg;
                outboundQueue.cancel(request.receiverId);
            }
            // 关闭连接请求
            else if (msgClazz == CloseRequest.class) {
                outboundQueue.close();
            }
            // 增加 Credit：下游有更多缓冲区可接收数据
            else if (msgClazz == AddCredit.class) {
                AddCredit request = (AddCredit) msg;
                outboundQueue.addCreditOrResumeConsumption(
                        request.receiverId, reader -> reader.addCredit(request.credit));
            }
            // 恢复消费：Checkpoint 对齐后恢复数据传输
            else if (msgClazz == ResumeConsumption.class) {
                ResumeConsumption request = (ResumeConsumption) msg;
                outboundQueue.addCreditOrResumeConsumption(
                        request.receiverId, NetworkSequenceViewReader::resumeConsumption);
            }
            // 确认所有用户记录已处理
            else if (msgClazz == AckAllUserRecordsProcessed.class) {
                AckAllUserRecordsProcessed request = (AckAllUserRecordsProcessed) msg;
                outboundQueue.acknowledgeAllRecordsProcessed(request.receiverId);
            }
            // 通知新的缓冲区大小
            else if (msgClazz == NewBufferSize.class) {
                NewBufferSize request = (NewBufferSize) msg;
                outboundQueue.notifyNewBufferSize(request.receiverId, request.bufferSize);
            }
            // 通知所需的 Segment ID（用于 Hybrid Shuffle）
            else if (msgClazz == SegmentId.class) {
                SegmentId request = (SegmentId) msg;
                outboundQueue.notifyRequiredSegmentId(
                        request.receiverId, request.subpartitionId, request.segmentId);
            } else {
                LOG.warn("Received unexpected client request: {}", msg);
            }
        } catch (Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
    }

    private void respondWithError(
            ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
        LOG.debug("Responding with error: {}.", error.getClass());

        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
    }
}
