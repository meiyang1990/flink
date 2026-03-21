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

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for filling in the content of the {@link MemorySegment}. To access written
 * data please use {@link BufferConsumer} which allows to build {@link Buffer} instances from the
 * written data.
 *
 * <h2>缓冲区构建器 - 中文说明</h2>
 *
 * <p>BufferBuilder 是 Flink 网络栈中用于向 {@link MemorySegment} 写入数据的核心类。
 * 它与 {@link BufferConsumer} 配对使用，实现生产者-消费者模式的数据传输。
 *
 * <h3>核心设计特点</h3>
 * <ul>
 *   <li><b>非线程安全</b>：单线程写入，不需要同步开销</li>
 *   <li><b>写入与读取分离</b>：写线程使用 BufferBuilder，读线程使用 BufferConsumer</li>
 *   <li><b>延迟提交</b>：写入的数据需要 commit 后才对消费者可见</li>
 *   <li><b>位置标记机制</b>：通过 PositionMarker 在生产者和消费者之间同步进度</li>
 * </ul>
 *
 * <h3>使用模式</h3>
 * <pre>
 *  // 生产者线程
 *  BufferBuilder builder = pool.requestBufferBuilder();
 *  builder.append(data);
 *  builder.commit();  // 使数据对消费者可见
 *  builder.finish();  // 标记写入完成
 *
 *  // 消费者线程（可以是不同线程）
 *  BufferConsumer consumer = builder.createBufferConsumer();
 *  Buffer buffer = consumer.build();  // 读取已提交的数据
 * </pre>
 *
 * <h3>典型使用场景</h3>
 * <ul>
 *   <li>RecordWriter 序列化记录后写入 BufferBuilder</li>
 *   <li>ResultSubpartition 从 BufferBuilder 获取 BufferConsumer 供下游读取</li>
 *   <li>Checkpoint Barrier 写入网络缓冲区</li>
 * </ul>
 *
 * @see BufferConsumer 用于读取 BufferBuilder 写入的数据
 * @see MemorySegment 底层内存存储
 */
@NotThreadSafe
public class BufferBuilder implements AutoCloseable {
    /**
     * 底层的 Buffer 包装器。
     * 实际类型是 NetworkBuffer，封装了 MemorySegment 和引用计数管理。
     */
    private final Buffer buffer;

    /**
     * 底层内存段。
     * 数据直接写入此内存区域，大小通常为 32KB。
     */
    private final MemorySegment memorySegment;

    /**
     * 缓冲区的最大可用容量。
     * 可以通过 trim() 方法缩小，但不能超过 MemorySegment 的原始大小。
     */
    private int maxCapacity;

    /**
     * 写入位置标记器。
     * 追踪当前写入位置，并通过 volatile 机制与消费者同步。
     * 负值表示缓冲区已完成（finished）。
     */
    private final SettablePositionMarker positionMarker = new SettablePositionMarker();

    /**
     * BufferConsumer 创建标志。
     * 每个 BufferBuilder 只能创建一个 BufferConsumer，防止数据竞争。
     */
    private boolean bufferConsumerCreated = false;

    public BufferBuilder(MemorySegment memorySegment, BufferRecycler recycler) {
        this.memorySegment = checkNotNull(memorySegment);
        this.buffer = new NetworkBuffer(memorySegment, recycler);
        this.maxCapacity = buffer.getMaxCapacity();
    }

    /**
     * This method always creates a {@link BufferConsumer} starting from the current writer offset.
     * Data written to {@link BufferBuilder} before creation of {@link BufferConsumer} won't be
     * visible for that {@link BufferConsumer}.
     *
     * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}.
     */
    public BufferConsumer createBufferConsumer() {
        return createBufferConsumer(positionMarker.cachedPosition);
    }

    /**
     * This method always creates a {@link BufferConsumer} starting from position 0 of {@link
     * MemorySegment}.
     *
     * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}.
     */
    public BufferConsumer createBufferConsumerFromBeginning() {
        return createBufferConsumer(0);
    }

    private BufferConsumer createBufferConsumer(int currentReaderPosition) {
        checkState(
                !bufferConsumerCreated, "Two BufferConsumer shouldn't exist for one BufferBuilder");
        bufferConsumerCreated = true;
        return new BufferConsumer(buffer.retainBuffer(), positionMarker, currentReaderPosition);
    }

    /** Gets the data type of the internal buffer. */
    public Buffer.DataType getDataType() {
        return buffer.getDataType();
    }

    /** Sets the data type of the internal buffer. */
    public void setDataType(Buffer.DataType dataType) {
        buffer.setDataType(dataType);
    }

    /** Same as {@link #append(ByteBuffer)} but additionally {@link #commit()} the appending. */
    public int appendAndCommit(ByteBuffer source) {
        int writtenBytes = append(source);
        commit();
        return writtenBytes;
    }

    /**
     * Append as many data as possible from {@code source}. Not everything might be copied if there
     * is not enough space in the underlying {@link MemorySegment}
     *
     * @return number of copied bytes
     */
    public int append(ByteBuffer source) {
        checkState(!isFinished());

        int needed = source.remaining();
        int available = getMaxCapacity() - positionMarker.getCached();
        int toCopy = Math.min(needed, available);

        memorySegment.put(positionMarker.getCached(), source, toCopy);
        positionMarker.move(toCopy);
        return toCopy;
    }

    /**
     * Make the change visible to the readers. This is costly operation (volatile access) thus in
     * case of bulk writes it's better to commit them all together instead one by one.
     *
     * <p>将写入的数据提交，使其对消费者可见。
     *
     * <p>这是一个相对昂贵的操作（涉及 volatile 写），因此在批量写入时
     * 应该累积多次写入后一次性提交，而不是每次写入都提交。
     */
    public void commit() {
        positionMarker.commit();
    }

    /**
     * Mark this {@link BufferBuilder} and associated {@link BufferConsumer} as finished - no new
     * data writes will be allowed.
     *
     * <p>This method should be idempotent to handle failures and task interruptions. Check
     * FLINK-8948 for more details.
     *
     * <p>标记缓冲区写入完成，不再接受新数据。
     *
     * <p>完成标记通过将位置值取负来表示（例如位置 100 变为 -100）。
     * 此方法需要支持幂等调用，以应对 Task 失败和中断恢复场景。
     *
     * @return number of written bytes.
     */
    public int finish() {
        // 标记完成并返回已写入字节数
        int writtenBytes = positionMarker.markFinished();
        commit();
        return writtenBytes;
    }

    public boolean isFinished() {
        return positionMarker.isFinished();
    }

    public boolean isFull() {
        checkState(positionMarker.getCached() <= getMaxCapacity());
        return positionMarker.getCached() == getMaxCapacity();
    }

    public int getWritableBytes() {
        checkState(positionMarker.getCached() <= getMaxCapacity());
        return getMaxCapacity() - positionMarker.getCached();
    }

    public int getCommittedBytes() {
        return positionMarker.getCached();
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * The result capacity can not be greater than allocated memorySegment. It also can not be less
     * than already written data.
     */
    public void trim(int newSize) {
        maxCapacity =
                Math.min(Math.max(newSize, positionMarker.getCached()), buffer.getMaxCapacity());
    }

    @Override
    public void close() {
        buffer.recycleBuffer();
    }

    /**
     * Holds a reference to the current writer position. Negative values indicate that writer
     * ({@link BufferBuilder} has finished. Value {@code Integer.MIN_VALUE} represents finished
     * empty buffer.
     *
     * <p>位置标记接口，用于在写入者和读取者之间同步进度。
     *
     * <p>位置值的含义：
     * <ul>
     *   <li><b>正值</b>：当前写入位置（字节偏移量）</li>
     *   <li><b>负值</b>：写入已完成，绝对值为最终位置</li>
     *   <li><b>Integer.MIN_VALUE</b>：特殊值，表示完成的空缓冲区</li>
     * </ul>
     */
    @ThreadSafe
    interface PositionMarker {
        /** 表示空缓冲区完成的特殊值 */
        int FINISHED_EMPTY = Integer.MIN_VALUE;

        /** 获取当前位置（volatile 读） */
        int get();

        /** 判断给定位置值是否表示已完成 */
        static boolean isFinished(int position) {
            return position < 0;
        }

        /** 获取位置的绝对值，用于计算实际字节数 */
        static int getAbsolute(int position) {
            if (position == FINISHED_EMPTY) {
                return 0;
            }
            return Math.abs(position);
        }
    }

    /**
     * Cached writing implementation of {@link PositionMarker}.
     *
     * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be
     * implemented independently of one another - so that the cached values can not accidentally
     * leak from one to another.
     *
     * <p>Remember to commit the {@link SettablePositionMarker} to make the changes visible.
     *
     * <p>带缓存的位置标记实现，用于 BufferBuilder（写入方）。
     *
     * <p>设计要点：
     * <ul>
     *   <li><b>本地缓存</b>：cachedPosition 避免频繁的 volatile 读写</li>
     *   <li><b>延迟可见</b>：修改 cachedPosition 后需调用 commit() 才对消费者可见</li>
     *   <li><b>独立缓存</b>：与 BufferConsumer 的缓存完全独立，防止缓存泄漏</li>
     * </ul>
     */
    static class SettablePositionMarker implements PositionMarker {
        /**
         * volatile 位置变量，用于跨线程同步。
         * 消费者通过读取此变量获取最新的已提交位置。
         */
        private volatile int position = 0;

        /**
         * Locally cached value of volatile {@code position} to avoid unnecessary volatile accesses.
         *
         * <p>本地缓存位置，减少 volatile 访问开销。
         * 写入时修改此值，调用 commit() 后同步到 volatile position。
         */
        private int cachedPosition = 0;

        @Override
        public int get() {
            return position;
        }

        /** 判断是否已完成（基于缓存值） */
        public boolean isFinished() {
            return PositionMarker.isFinished(cachedPosition);
        }

        /** 获取缓存位置的绝对值 */
        public int getCached() {
            return PositionMarker.getAbsolute(cachedPosition);
        }

        /**
         * Marks this position as finished and returns the current position.
         *
         * <p>标记为完成状态：将当前位置取负。
         * 如果当前位置是 0，使用 FINISHED_EMPTY 特殊值。
         *
         * @return current position as of {@link #getCached()}
         */
        public int markFinished() {
            int currentPosition = getCached();
            int newValue = -currentPosition;
            // 位置 0 取负仍是 0，需要特殊处理
            if (newValue == 0) {
                newValue = FINISHED_EMPTY;
            }
            set(newValue);
            return currentPosition;
        }

        /** 移动位置指针 */
        public void move(int offset) {
            set(cachedPosition + offset);
        }

        /** 设置缓存位置（不会立即对消费者可见） */
        public void set(int value) {
            cachedPosition = value;
        }

        /**
         * 提交位置变更，使其对消费者可见。
         * 这是唯一写入 volatile position 的地方。
         */
        public void commit() {
            position = cachedPosition;
        }
    }
}
