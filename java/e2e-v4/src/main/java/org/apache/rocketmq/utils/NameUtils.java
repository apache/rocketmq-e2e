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

package org.apache.rocketmq.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NameUtils {
    /**
     * Make sure not used in other cases.
     */
    private static Map<String, String> alreadyUsed = new ConcurrentHashMap<>();

    public synchronized static String getTopicName() {
        while (true) {
            String topic = "topic-server-" + RandomStringUtils.randomAlphanumeric(20);
            String used = alreadyUsed.putIfAbsent(topic, topic);
            if (used == null) {
                return topic;
            }
        }

    }

    public synchronized static String getGroupName() {
        while (true) {
            String gid = "GID-server-" + RandomStringUtils.randomAlphanumeric(20);
            String used = alreadyUsed.putIfAbsent(gid, gid);
            if (used == null) {
                return gid;
            }
        }

    }

    public synchronized static String getTagName() {
        while (true) {
            String tag = "tag-server-" + RandomStringUtils.randomAlphanumeric(20);
            String used = alreadyUsed.putIfAbsent(tag, tag);
            if (used == null) {
                return tag;
            }
        }

    }

    public synchronized static String getRandomTagName() {
        while (true) {
            String tag = "tag-server-" + RandomStringUtils.randomAlphanumeric(20);
            String used = alreadyUsed.putIfAbsent(tag, tag);
            if (used == null) {
                return tag;
            }
        }
    }

    public synchronized static String getRandomGroupName() {
        while (true) {
            String gid = "GID-server-" + RandomStringUtils.randomAlphanumeric(20);
            String used = alreadyUsed.putIfAbsent(gid, gid);
            if (used == null) {
                return gid;
            }
        }
    }

    protected static String getMD5Sum(String className, String methodName) {
        String completeName = String.format("%s-%s", className, methodName);
        return methodName + "-" + DigestUtils.md5Hex(completeName).substring(0, 6);
    }

}
