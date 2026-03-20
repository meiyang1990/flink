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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.asyncprocessing.AsyncFutureImpl;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link AsyncFutureImpl.CallbackRunner} that gives info of {@link #isHasMail()} to the AEC and
 * notifies new mail if needed.
 */
public class CallbackRunnerWrapper {

    private final MailboxExecutor mailboxExecutor;

    /** Counter of current callbacks. */
    private final AtomicInteger currentCallbacks = new AtomicInteger(0);

    /** The logic to notify new mails to AEC. */
    private final Runnable newMailNotify;

    CallbackRunnerWrapper(MailboxExecutor mailboxExecutor, Runnable newMailNotify) {
        this.mailboxExecutor = mailboxExecutor;
        this.newMailNotify = newMailNotify;
    }

    /**
     * Submit a callback to run.
     *
     * @param task the callback.
     */
    public void submit(ThrowingRunnable<? extends Exception> task) {
        // 【学习型注释】先提交到 Mailbox，再递增计数。使用 CAS 语义确保从 0→1 时触发通知。
        // 先 submit 再 increment 的顺序很重要：如果先 increment 再 submit，
        // 可能在 submit 之前 isHasMail 就返回 true，但回调还未实际入队。
        mailboxExecutor.execute(
                () -> {
                    // 【学习型注释】先 decrement 再执行任务，防止任务中查询 isHasMail 时误判
                    currentCallbacks.decrementAndGet();
                    task.run();
                },
                "Callback of state request");
        if (currentCallbacks.getAndIncrement() == 0) {
            notifyNewMail();
        }
    }

    private void notifyNewMail() {
        if (newMailNotify != null) {
            newMailNotify.run();
        }
    }

    public boolean isHasMail() {
        return currentCallbacks.get() > 0;
    }
}
