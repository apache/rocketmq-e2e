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

package org.apache.rocketmq.frame;

import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseOperate extends ResourceInit {
    private static Logger logger = LoggerFactory.getLogger(BaseOperate.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                MQAdmin.mqAdminExt.shutdown();
                logger.info("Shutdown Hook is running !");
            }
        });
    }

    protected static String getTopic(String methodName) {
        String topic = String.format("topic_%s_%s", methodName, RandomUtils.getStringWithCharacter(6));
        logger.info("[Topic] topic:{}, methodName:{}", topic, methodName);
        boolean result = MQAdmin.createTopic(namesrvAddr,cluster, topic, 8);
        Assertions.assertTrue(result, String.format("Create topic:%s failed", topic));
        return topic;
    }

    protected static String getGroupId(String methodName) {
        String groupId = String.format("GID_%s_%s", methodName, RandomUtils.getStringWithCharacter(6));
        logger.info("[ConsumerGroupId] groupId:{}, methodName:{}", groupId, methodName);
        return groupId;
    }
}
