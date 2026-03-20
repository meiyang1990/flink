/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// 这个文件已经全部加上中文注释

package org.apache.flink.runtime.blocklist;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

/**
 * No-op implementation of {@link BlocklistHandler}.
 *
 * <p>【学习型注释】
 * NoOpBlocklistHandler 是 BlocklistHandler 的空实现，所有方法都是空操作。
 *
 * <p>使用场景：
 * 当屏蔽功能未启用时（如未开启推测执行），ResourceManager 会使用此实现，
 * 避免屏蔽列表相关逻辑对系统产生影响。
 *
 * <p>设计模式：
 * 空对象模式（Null Object Pattern），避免在调用方进行 null 检查。
 */
public class NoOpBlocklistHandler implements BlocklistHandler {
    @Override
    public void addNewBlockedNodes(Collection<BlockedNode> newNodes) {}

    @Override
    public boolean isBlockedTaskManager(ResourceID taskManagerId) {
        return false;
    }

    @Override
    public Set<String> getAllBlockedNodeIds() {
        return Collections.emptySet();
    }

    @Override
    public void registerBlocklistListener(BlocklistListener blocklistListener) {}

    @Override
    public void deregisterBlocklistListener(BlocklistListener blocklistListener) {}

    /** The factory to instantiate {@link NoOpBlocklistHandler}. */
    public static class Factory implements BlocklistHandler.Factory {

        @Override
        public BlocklistHandler create(
                BlocklistContext blocklistContext,
                Function<ResourceID, String> taskManagerNodeIdRetriever,
                ComponentMainThreadExecutor mainThreadExecutor,
                Logger log) {
            return new NoOpBlocklistHandler();
        }
    }
}
