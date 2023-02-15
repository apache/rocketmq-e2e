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

import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ClientConfigurationFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.util.NameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.NORMAL)
@DisplayName("Test cases that send messages")
public class NormalMessageSizeTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(NormalMessageSizeTest.class);
    private static String normalTopic;
    private static String transTopic;
    private static String fifoTopic;
    private static String delayTopic;
    private static Producer producer;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        normalTopic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        transTopic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        delayTopic = getTopic(TopicMessageType.DELAY.getValue(), methodName);
        fifoTopic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        try {
            producer = provider.newProducerBuilder()
                .setTransactionChecker(messageView -> TransactionResolution.COMMIT)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(normalTopic, transTopic, delayTopic, fifoTopic)
                .build();
        } catch (ClientException e) {
            Assertions.fail("create producer failed");
        }
    }

    @AfterAll
    public static void tearDownAll() {
        if (producer != null) {
            try {
                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @DisplayName("Send normal messages synchronously with the body size of 4M+1, expect send failed")
    public void testNormalMsgSize4MAdd1() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(normalTopic, tag, messageBody);
            producer.send(message);
        });
    }

    @Test
    @DisplayName("Send normal messages synchronously with the body size of 4M, expect send success")
    public void testNormalMsgSize4M() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = MessageFactory.buildMessage(normalTopic, tag, messageBody);
        try {
            producer.send(message);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Send message failed, expected success");
        }
    }

    @Test
    @DisplayName("Send delay messages synchronously with the body size of 4M+1, expect send failed")
    public void testDelayMsgSize4MAdd1() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildDelayMessage(delayTopic, tag, messageBody, 10L);
            producer.send(message);
        });
    }

    @Test
    @DisplayName("Send delay messages synchronously with the body size of 4M, expect send success")
    public void testDelayMsgSize4M() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = MessageFactory.buildDelayMessage(delayTopic, tag, messageBody, 10L);
        try {
            producer.send(message);
        } catch (ClientException e) {
            e.printStackTrace();
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send transaction messages synchronously with the body size of 4M+1, expect send failed")
    public void testTransMsgSize4MAdd1() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildMessage(transTopic, tag, messageBody);
            Transaction transaction = producer.beginTransaction();
            producer.send(message, transaction);
            transaction.commit();
        });
    }

    @Test
    @DisplayName("Send transaction messages synchronously with the body size of 4M, expect send success")
    public void testTransMsgSize4M() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = MessageFactory.buildMessage(transTopic, tag, messageBody);
        try {
            Transaction transaction = producer.beginTransaction();
            producer.send(message, transaction);
            transaction.commit();
        } catch (ClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send FIFO messages synchronously with the body size of 4M+1, expect send failed")
    public void testFifoMsgSize4MAdd1() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = MessageFactory.buildOrderMessage(fifoTopic, tag, messageBody, "a");
            producer.send(message);
        });
    }

    @Test
    @DisplayName("Send FIFO messages synchronously with the body size of 4M, expect send success")
    public void testFifoMsgSize4M() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = MessageFactory.buildOrderMessage(fifoTopic, tag, messageBody, "a");
        try {
            producer.send(message);
        } catch (ClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send normal messages synchronously with the body size of 4M and the user property size of 16KB, expect send success")
    public void testNormalMsgSize4MAndUserProperty16KB() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String key = RandomStringUtils.randomAlphabetic(8 * 1024);
        String value = RandomStringUtils.randomAlphabetic(8 * 1024);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);
        try {
            Message message = MessageFactory.buildMessageWithProperty(normalTopic, messageBody, userProperty);
            producer.send(message);
        } catch (ClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send FIFO messages synchronously with the body size of 4M and the user property size of 16KB, expect send success")
    public void testFifoMsgSize4MAndUserProperty16KB() {
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String key = RandomStringUtils.randomAlphabetic(8 * 1024);
        String value = RandomStringUtils.randomAlphabetic(8 * 1024);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);
        try {
            Message message = MessageFactory.buildOrderMessageWithProperty(fifoTopic, messageBody, "a", userProperty);
            producer.send(message);
        } catch (ClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

}
