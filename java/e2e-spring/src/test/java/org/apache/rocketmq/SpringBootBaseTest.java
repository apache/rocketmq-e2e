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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;


@Slf4j
@SpringBootTest(classes = RocketMQApplication.class)
public class SpringBootBaseTest {
    @Value("${rocketmq.cluster}")
    private String cluster;

    protected String getTopic(String messageType, String methodName) {
        String topic = String.format("topic_%s_%s_%s", messageType, methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[Topic] topic:{}, messageType:{}, methodName:{}", topic, messageType, methodName);
        boolean result = MQAdmin.createTopic(cluster, topic, 8, messageType);
        Assertions.assertTrue(result, String.format("Create topic:%s failed", topic));
        return topic;
    }

    protected String getGroupId(String methodName) {
        String groupId = String.format("GID_%s_%s", methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[ConsumerGroupId] groupId:{}, methodName:{}", groupId, methodName);
        return groupId;
    }

}
