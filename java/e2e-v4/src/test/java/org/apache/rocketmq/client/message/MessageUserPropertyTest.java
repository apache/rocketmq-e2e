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

package org.apache.rocketmq.client.message;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test user property
 */
@Tag(TESTSET.CLIENT)
public class MessageUserPropertyTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(MessageUserPropertyTest.class);
    private static String topic;
    private static RMQNormalProducer producer;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(methodName);
        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
    }

    @AfterAll
    public static void tearDownAll() {
        if (producer != null) {
            producer.shutdown();
        }
    }

    @Disabled
    @DisplayName("Message user property equals limit 31KB, expect send success")
    public void testMessageUserPropertyEquals16KB() {
        String key = RandomStringUtils.randomAlphabetic(31 * 512);
        String value = RandomStringUtils.randomAlphabetic(31 * 512);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);

        Message message = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        for (Map.Entry<String, String> entry : userProperty.entrySet()) {
            message.putUserProperty(entry.getKey(), entry.getValue());
        }
        producer.send(message);
    }

    @Disabled
    @DisplayName("Message user property limit 32KB ,expect throw exception")
    public void testMessageUserPropertyBeyond16KB() {
        String key = RandomStringUtils.randomAlphabetic(16 * 1024);
        String value = RandomStringUtils.randomAlphabetic(16 * 1024);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);

        assertThrows(Exception.class, () -> {
            Message message = new Message(topic, RandomUtils.getStringByUUID().getBytes());
            for (Map.Entry<String, String> entry : userProperty.entrySet()) {
                message.putUserProperty(entry.getKey(), entry.getValue());
            }
            producer.getProducer().send(message);
        }, " message user property beyond 16KB ,expect throw exception but it didn't");
    }

}
