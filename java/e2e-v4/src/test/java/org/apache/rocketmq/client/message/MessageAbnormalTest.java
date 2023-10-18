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

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test message properties
 */
@Tag(TESTSET.CLIENT)
public class MessageAbnormalTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(MessageAbnormalTest.class);
    private static DefaultMQProducer producer;
    private static String topic;
    private String tag;

    @BeforeAll
    public static void setUpAll() {
        String className = MessageAbnormalTest.class.getName();
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(methodName);
        try {
            producer = new DefaultMQProducer("MessageAbnormalTest", rpcHook);
            producer.setNamesrvAddr(namesrvAddr);
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @AfterEach
    public void tearDown() {
    }

    @AfterAll
    public static void tearDownAll() {

    }

    // TODO
    @Disabled
    @DisplayName("producer invoke send(messageBody=\"\"), expect throw exception")
    public void sendMsgBodyIsEmpty() {
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(topic, tag, "");
            producer.send(message);
        }, "Send messages with a null character message, Expected send() to throw exception, but it didn't");
    }

    @Test
    @DisplayName("producer invoke send(messageBody=null), expect build message throw exception")
    public void sendMsgBodyIsNull() {
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(topic, tag, null);
            producer.send(message);
        }, "Send messages with a null character message, Expected build() to throw exception, but it didn't");
    }

    @Test
    @DisplayName("producer invoke send(topic=\"\"), expect throw exception")
    public void sendMsgTopicIsEmpty() {
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage("", tag, RandomUtils.getStringByUUID());
            producer.send(message);
        }, "Topic does not exist, Expected build() to throw exception, but it didn't");
    }

    @Test
    @DisplayName("producer invoke send(topic=null), expect throw exception")
    public void sendMsgTopicIsNull() {
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(null, tag, RandomUtils.getStringByUUID());
            producer.send(message);
        }, "Topic does not exist,Expected build() to throw exception, but it didn't");
    }

    @Disabled
    @DisplayName("producer invoke send(tag=null), expect build message throw exception")
    public void sendMsgTagIsNull() {
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(topic, null, RandomUtils.getStringByUUID());
            producer.send(message);
        }, "tag is null, Expected build() to throw exception, but it didn't");
    }

    @Disabled
    @DisplayName("producer invoke send(tag=\"\"), expect build message throw exception")
    public void sendMsgTagIsEmpty() {
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(topic, "", RandomUtils.getStringByUUID());
            producer.send(message);
        }, "tag is blank, Expected build() to throw exception, but it didn't");
    }
}
