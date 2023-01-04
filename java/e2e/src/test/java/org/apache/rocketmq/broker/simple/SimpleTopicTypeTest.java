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
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.client.trans.LocalTransactionCheckerImpl;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.SIMPLE)
public class SimpleTopicTypeTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(SimpleTopicTypeTest.class);
    private String tag;
    private String groupId;
    private final static int SEND_NUM = 20;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
        groupId = NameUtils.getRandomGroupName();
        log.info("tag:{}, groupId:{}", tag, groupId);
    }

    @AfterEach
    public void tearDown() {
    }

    //TODO
    @Disabled
    @DisplayName("Send 10 delayed messages synchronously and expect SimpleConsumer to receive() and ack() messages properly")
    public void testDelay_simple_receive_ack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(TopicMessageType.DELAY.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildDelayMessage(topic, tag, String.valueOf(i), System.currentTimeMillis() + 10 * 1000);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitDelayReceiveThenAck(producer, consumer, 1, 10000);
    }

    @Test
    @DisplayName("Send 10 transaction messages synchronously and expect SimpleConsumer to receive() and ack() messages properly")
    public void testTrans_simple_receive_ackAsync() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQTransProducer(account, topic, new LocalTransactionCheckerImpl(TransactionResolution.COMMIT));
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, String.valueOf(i));
            producer.sendTrans(message, true);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitReceiveThenAckAsync(producer, consumer, 1, Duration.ofSeconds(30));
    }

}

