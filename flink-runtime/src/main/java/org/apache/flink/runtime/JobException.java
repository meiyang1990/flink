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

package org.apache.flink.runtime;

import org.apache.flink.util.FlinkException;

/** Indicates that a job has failed. */
// 【学习型注释】作业级别的异常，用于表示 Flink 作业执行过程中发生的错误
public class JobException extends FlinkException {

    private static final long serialVersionUID = 1275864691743020176L;

    public JobException(String msg) {
        super(msg);
    }

    public JobException(String message, Throwable cause) {
        super(message, cause);
    }
}
