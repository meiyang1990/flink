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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder.PositionMarker;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for producing {@link Buffer}.
 *
 * <p>It reads data written by {@link BufferBuilder}. Although it is not thread safe and can be used
 * only by one single thread, this thread can be different than the thread using/writing to {@link
 * BufferBuilder}. Pattern here is simple: one thread writes data to {@link BufferBuilder} and there
 * can be a different thread reading from it using {@link BufferConsumer}.
 *
 * <h2>缓冲区消费者 - 中文说明</h2>
 *
 * <p>BufferConsumer 是 {@link BufferBuilder} 的读取端对应物，用于消费已写入的数据并生成 {@link Buffer} 实例。
 *
 * <h3>核心设计特点</h3>
 * <ul>
 *   <li><b>非线程安全但可跨线程</b>：单线程使用，但可以与 BufferBuilder 在不同线程</li>
 *   <li><b>增量读取</b>：每次 build() 只返回自上次读取以来新写入的数据</li>
 *   <li><b>引用计数共享</b>：与源 Buffer 共享引用计数，两者都需要释放</li>
 *   <li><b>可复制</b>：通过 copy() 创建独立索引的副本，支持多次读取同一数据</li>
 * </ul>
 *
 * <h3>与 BufferBuilder 的协作模式</h3>
 * <pre>
 *  生产者线程              消费者线程
 *  ──────────              ──────────
 *  append(data1)
 *  commit()
 *                          build() → 返回 data1
 *  append(data2)
 *  commit()
 *                          build() → 返回 data2
 *  finish()
 *                          isFinished() → true
 *                          build() → 空或剩余数据
 * </pre>
 *
 * <h3>典型使用场景</h3>
 * <ul>
 *   <li>ResultSubpartition 持有 BufferConsumer，供下游请求数据</li>
 *   <li>网络传输层从 BufferConsumer 构建发送缓冲区</li>
 *   <li>Checkpoint 写入时复制 BufferConsumer 保留数据快照</li>
 * </ul>
 *
 * @see BufferBuilder 用于向缓冲区写入数据
 * @see Buffer 最终生成的可发送缓冲区
 */
@NotThreadSafe
public class BufferConsumer implements Closeable {
    /**
     * 底层缓冲区，与 BufferBuilder 共享同一个 MemorySegment。
     * 通过引用计数管理生命周期，BufferConsumer 关闭时会释放引用。
     */
    private final Buffer buffer;

    /**
     * 写入者位置的缓存包装器。
     * 用于获取 BufferBuilder 当前的写入进度，判断是否有新数据可读。
     */
    private final CachedPositionMarker writerPosition;

    /**
     * 当前读取位置。
     * 每次 build() 后更新为写入者的位置，下次 build() 从此处开始读取。
     */
    private int currentReaderPosition;

    /** Constructs {@link BufferConsumer} instance with static content of a certain size. */
    public BufferConsumer(Buffer buffer, int size) {
        this(buffer, () -> -size, 0);
        checkState(
                isFinished(),
                "BufferConsumer with static size must be finished after construction!");
    }

    public BufferConsumer(
            Buffer buffer,
            BufferBuilder.PositionMarker currentWriterPosition,
            int currentReaderPosition) {
        this.buffer = checkNotNull(buffer);
        this.writerPosition = new CachedPositionMarker(checkNotNull(currentWriterPosition));
        checkArgument(
                currentReaderPosition <= writerPosition.getCached(),
                "Reader position larger than writer position");
        this.currentReaderPosition = currentReaderPosition;
    }

    /**
     * Checks whether the {@link BufferBuilder} has already been finished.
     *
     * <p>BEWARE: this method accesses the cached value of the position marker which is only updated
     * after calls to {@link #build()} and {@link #skip(int)}!
     *
     * @return <tt>true</tt> if the buffer was finished, <tt>false</tt> otherwise
     */
    public boolean isFinished() {
        return writerPosition.isFinished();
    }

    /**
     * @return sliced {@link Buffer} containing the not yet consumed data. Returned {@link Buffer}
     *     shares the reference counter with the parent {@link BufferConsumer} - in order to recycle
     *     memory both of them must be recycled/closed.
     *
     * <p>构建并返回自上次读取以来新写入的数据。
     *
     * <p>实现要点：
     * <ul>
     *   <li>先更新写入者位置缓存，获取最新的可读范围</li>
     *   <li>创建 [currentReaderPosition, writerPosition) 范围的只读切片</li>
     *   <li>更新 currentReaderPosition 为当前写入位置</li>
     *   <li>返回的 Buffer 与原 Buffer 共享引用计数</li>
     * </ul>
     *
     * <p>注意：如果写入者尚未 commit 新数据，可能返回空的 Buffer。
     */
    public Buffer build() {
        // 刷新写入者位置缓存
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        // 创建只读切片：从当前读取位置到写入位置
        Buffer slice =
                buffer.readOnlySlice(
                        currentReaderPosition, cachedWriterPosition - currentReaderPosition);
        // 更新读取位置，下次从新位置开始
        currentReaderPosition = cachedWriterPosition;
        // 增加引用计数并返回
        return slice.retainBuffer();
    }

    /**
     * @param bytesToSkip number of bytes to skip from currentReaderPosition
     */
    void skip(int bytesToSkip) {
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        int bytesReadable = cachedWriterPosition - currentReaderPosition;
        checkState(bytesToSkip <= bytesReadable, "bytes to skip beyond readable range");
        currentReaderPosition += bytesToSkip;
    }

    /**
     * Returns a retained copy with separate indexes. This allows to read from the same {@link
     * MemorySegment} twice.
     *
     * <p>WARNING: the newly returned {@link BufferConsumer} will have its reader index copied from
     * the original buffer. In other words, data already consumed before copying will not be visible
     * to the returned copies.
     *
     * @return a retained copy of self with separate indexes
     */
    public BufferConsumer copy() {
        return new BufferConsumer(
                buffer.retainBuffer(), writerPosition.positionMarker, currentReaderPosition);
    }

    /**
     * Returns a retained copy with separate indexes and sets the reader position to the given
     * value. This allows to read from the same {@link MemorySegment} twice starting from the
     * supplied position.
     *
     * @param readerPosition the new reader position. Can be less than the {@link
     *     #currentReaderPosition}, but may not exceed the current writer's position.
     * @return a retained copy of self with separate indexes
     */
    public BufferConsumer copyWithReaderPosition(int readerPosition) {
        return new BufferConsumer(
                buffer.retainBuffer(), writerPosition.positionMarker, readerPosition);
    }

    public boolean isBuffer() {
        return buffer.isBuffer();
    }

    public Buffer.DataType getDataType() {
        return buffer.getDataType();
    }

    @Override
    public void close() {
        if (!buffer.isRecycled()) {
            buffer.recycleBuffer();
        }
    }

    public boolean isRecycled() {
        return buffer.isRecycled();
    }

    public int getWrittenBytes() {
        return writerPosition.getCached();
    }

    int getCurrentReaderPosition() {
        return currentReaderPosition;
    }

    boolean isStartOfDataBuffer() {
        return buffer.getDataType() == DATA_BUFFER && currentReaderPosition == 0;
    }

    int getBufferSize() {
        return buffer.getMaxCapacity();
    }

    /** Returns true if there is new data available for reading. */
    public boolean isDataAvailable() {
        return currentReaderPosition < writerPosition.getLatest();
    }

    public String toDebugString(boolean includeHash) {
        Buffer buffer = null;
        try (BufferConsumer copiedBufferConsumer = copy()) {
            buffer = copiedBufferConsumer.build();
            checkState(copiedBufferConsumer.isFinished());
            return buffer.toDebugString(includeHash);
        } finally {
            if (buffer != null) {
                buffer.recycleBuffer();
            }
        }
    }

    /**
     * Cached reading wrapper around {@link PositionMarker}.
     *
     * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be
     * implemented independently of one another - so that the cached values can not accidentally
     * leak from one to another.
     *
     * <p>带缓存的位置标记读取器，用于 BufferConsumer（读取方）。
     *
     * <p>设计要点：
     * <ul>
     *   <li><b>延迟更新</b>：只在调用 update() 时从 volatile 变量读取最新值</li>
     *   <li><b>独立缓存</b>：与 BufferBuilder 的 SettablePositionMarker 完全独立</li>
     *   <li><b>避免频繁同步</b>：连续的 isFinished()/getCached() 调用使用缓存值</li>
     * </ul>
     */
    private static class CachedPositionMarker {
        /** 底层位置标记（由 BufferBuilder 的 SettablePositionMarker 实现） */
        private final PositionMarker positionMarker;

        /**
         * Locally cached value of {@link PositionMarker} to avoid unnecessary volatile accesses.
         *
         * <p>本地缓存的位置值。
         * 调用 update() 后刷新，其他方法使用缓存值。
         */
        private int cachedPosition;

        CachedPositionMarker(PositionMarker positionMarker) {
            this.positionMarker = checkNotNull(positionMarker);
            update();
        }

        /** 判断是否已完成（基于缓存值） */
        public boolean isFinished() {
            return PositionMarker.isFinished(cachedPosition);
        }

        /** 获取缓存位置的绝对值（实际字节数） */
        public int getCached() {
            return PositionMarker.getAbsolute(cachedPosition);
        }

        /** 获取最新位置值（直接读取 volatile 变量） */
        private int getLatest() {
            return PositionMarker.getAbsolute(positionMarker.get());
        }

        /** 从 volatile 变量刷新缓存值 */
        private void update() {
            this.cachedPosition = positionMarker.get();
        }
    }
}
