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

package org.apache.rocketmq.broker.client.message;

import java.util.HashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        producer = ProducerFactory.getRMQProducer(account, topic);
    }

    @AfterAll
    public static void tearDownAll() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    @DisplayName("Message user property beyond limit 128 ,expect throw exception")
    public void testMessageUserPropertyBeyondSize128() {
        HashMap<String, String> userProperty = new HashMap<>();
        for (int i = 0; i <= 128; i++) {
            userProperty.put(String.valueOf(i), RandomStringUtils.randomAlphabetic(2));
        }
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProperty);
            producer.getProducer().send(message);
        }, " message user property beyond limit 128 ,expect throw exception but it didn't");
    }

    @Test
    @DisplayName("The number of message user propertys equals limit 128, expect send success")
    public void testMessageUserPropertyEqualsSize128() {
        HashMap<String, String> userProperty = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            userProperty.put(String.valueOf(i), RandomStringUtils.randomAlphabetic(2));
        }
        Message message = MessageFactory.buildMessageWithProperty(topic, userProperty);
        producer.send(message);
    }

    @Test
    @DisplayName("Message user property equals limit 16KB, expect send success")
    public void testMessageUserPropertyEquals16KB() {
        String key = RandomStringUtils.randomAlphabetic(8 * 1024);
        String value = RandomStringUtils.randomAlphabetic(8 * 1024);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);

        Message message = MessageFactory.buildMessageWithProperty(topic, userProperty);
        producer.send(message);
    }

    @Test
    @DisplayName("Message user property beyond 16KB ,expect throw exception")
    public void testMessageUserPropertyBeyond16KB() {
        String key = RandomStringUtils.randomAlphabetic(8 * 1024);
        String value = RandomStringUtils.randomAlphabetic(8 * 1024 + 1);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);

        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProperty);
            producer.getProducer().send(message);
        }, " message user property beyond 16KB ,expect throw exception but it didn't");
    }

    @Test
    @DisplayName("Message user property contains invisible character \u0000 ,expect throw exception")
    public void testMessageUserPropertyContentWithInvisibleCharacter() {
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put("\u0000", "value");

        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProperty);
            producer.getProducer().send(message);
        }, " message user property contains invisible character ,expect throw exception but it didn't");

    }

    @Test
    @DisplayName("Message user property use SystemKey UNIQ_KEY, expect throw exception")
    public void testMessageUserPropertyWithSystemKey() {
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put("UNIQ_KEY", "value");

        assertThrows(BadRequestException.class, () -> {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProperty);
            producer.getProducer().send(message);
        }, " message user property use system key UNIQ_KEY ,expect throw exception but it didn't");

    }

    @Test
    @DisplayName("Message user property ,key and tag beyond 16KB ,expect throw exception")
    public void testMessageUserPropertyKeyAndTagBeyond16KB() {
        HashMap<String, String> userProperty = new HashMap<>();
        String body = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String key = RandomStringUtils.randomAlphabetic(4 * 1024);
        String value = RandomStringUtils.randomAlphabetic(4 * 1024);
        userProperty.put(key, value);
        String tag = RandomStringUtils.randomAlphabetic(4 * 1024);
        String msgKey = RandomStringUtils.randomAlphabetic(4 * 1024 + 1);

        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessageWithProperty(topic, tag, userProperty, body, msgKey);
            producer.getProducer().send(message);
        }, "message user property ,key and tag beyond 16KB ,expect throw exception but it didn't");

    }

    @Test
    @DisplayName("Message user property ,key and tag equals 16KB, expect send success")
    public void testMessageUserPropertyKeyAndTagEquals16KB() {
        HashMap<String, String> userProperty = new HashMap<>();
        String body = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String key = RandomStringUtils.randomAlphabetic(4 * 1024);
        String value = RandomStringUtils.randomAlphabetic(4 * 1024);
        userProperty.put(key, value);
        String tag = RandomStringUtils.randomAlphabetic(4 * 1024);
        String msgKey = RandomStringUtils.randomAlphabetic(4 * 1024);

        Message message = MessageFactory.buildMessageWithProperty(topic, tag, userProperty, body, msgKey);
        producer.send(message);
    }

    @Test
    @DisplayName("Message user property ,key and tag equals 64B, expect send success")
    public void testMessageUserPropertyKeyAndTagEquals64B() {
        HashMap<String, String> userProperty = new HashMap<>();
        String body = RandomStringUtils.randomAlphabetic(64);
        String key = RandomStringUtils.randomAlphabetic(64);
        String value = RandomStringUtils.randomAlphabetic(64);
        userProperty.put(key, value);
        String tag = RandomStringUtils.randomAlphabetic(64);
        String msgKey = RandomStringUtils.randomAlphabetic(64);

        Message message = MessageFactory.buildMessageWithProperty(topic, tag, userProperty, body, msgKey);
        producer.send(message);

    }
    //BEGIN GENERATED CODE
    @Test
    @DisplayName("Message user property is the visible character, expect send success")
    public void testSpecialCharProperty() {
        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("中文", "中文");
        userProps.put("_", "_");
        userProps.put("%", "%");
        userProps.put("。", "。");
        userProps.put("||", "||");
        userProps.put("&&", "&&");
        userProps.put("🏷", "🏷");
        Message message = MessageFactory.buildMessageWithProperty(topic, userProps);
        producer.send(message);
    }
    //END GENERATED CODE
}


