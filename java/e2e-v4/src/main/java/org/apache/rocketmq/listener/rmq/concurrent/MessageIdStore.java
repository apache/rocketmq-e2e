/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.listener.rmq.concurrent;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageIdStore {
    private Set<String> consumedMessageIds;
    private static MessageIdStore instance;

    private MessageIdStore() {
        consumedMessageIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public static MessageIdStore getInstance() {
        if (instance == null) {
            synchronized (MessageIdStore.class) {
                if (instance == null) {
                    instance = new MessageIdStore();
                }
            }
        }
        return instance;
    }

    public boolean isMessageConsumed(String messageId) {
        return consumedMessageIds.contains(messageId);
    }

    public void markMessageAsConsumed(String messageId) {
        consumedMessageIds.add(messageId);
    }

    public void clear() {
        consumedMessageIds.clear();
    }
}
