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

package org.apache.rocketmq.broker.simple;

import java.time.Duration;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.TestUtils;
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.NORMAL)
@Tag(TESTSET.SIMPLE)
public class SimpleAckTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(SimpleAckTest.class);
    private String tag;
    private final static int SEND_NUM = 20;
    private RMQNormalProducer producer;

    @BeforeAll
    public static void setUpAll() {
    }

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
        log.info("tag:{}", tag);
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("Send 20 normal messages synchronously and expect SimpleConsumer to receive() and ack() messages normally")
    public void testNormal_simple_receive_ack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, tag + "-" + i);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitReceiveThenAck(producer, consumer, 1, Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("Send 20 normal messages synchronously. Expect SimpleConsumer to receive normal receiveAsync() and ack() messages")
    public void testNormal_simple_receiveAsync_ack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, tag + "-" + i);
            producer.sendAsync(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitReceiveAsyncThenAck(producer, consumer, 1, Duration.ofSeconds(30));
    }

    @Test
    @Timeout(180)
    @DisplayName("Send 20 normal messages synchronously and expect SimpleConsumer to receive() and ackAsync() messages normally")
    public void testNormal_simple_receive_ackAsync() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, tag + "-" + i);
            producer.sendAsync(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitReceiveThenAckAsync(producer, consumer, 1, Duration.ofSeconds(30));
    }

    @Test
    @Timeout(180)
    @DisplayName("Send 20 normal messages synchronously. Expect SimpleConsumer to receive normal receiveAsync() and ackAsync() messages")
    public void testNormal_simple_receiveAsync_ackAsync() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, tag + "-" + i);
            producer.sendAsync(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        VerifyUtils.waitReceiveAsyncThenAckAsync(producer, consumer, 1, Duration.ofSeconds(30));
    }

}

