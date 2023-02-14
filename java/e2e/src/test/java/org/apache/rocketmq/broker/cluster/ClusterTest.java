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

package org.apache.rocketmq.broker.cluster;

import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.RandomUtils;
import org.apache.rocketmq.util.TestUtils;
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Tag(TESTSET.MODEL)
public class ClusterTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(ClusterTest.class);
    private String tag;
    private final static int SEND_NUM = 100;
    private PushConsumer pushConsumer01;
    private PushConsumer pushConsumer02;
    private PushConsumer pushConsumer03;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @BeforeEach
    public void tearDown() {
        TestUtils.waitForSeconds(60);
        try {
            if (pushConsumer01 != null) {
                pushConsumer01.close();
            }
            if (pushConsumer02 != null) {
                pushConsumer02.close();
            }
            if (pushConsumer03 != null) {
                pushConsumer03.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Send 100 normal messages synchronously, start three consumers on different GroupId, and expect each client to consume up to 100 messages")
    public void testBroadcastConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId01 = getGroupId(methodName + 1);
        String groupId02 = getGroupId(methodName + 2);
        String groupId03 = getGroupId(methodName + 3);

        RMQNormalListener listenerA = new RMQNormalListener("ListenerA");
        RMQNormalListener listenerB = new RMQNormalListener("ListenerB");
        RMQNormalListener listenerC = new RMQNormalListener("ListenerC");
        pushConsumer01 = ConsumerFactory.getPushConsumer(account, topic, groupId01, new FilterExpression(tag), listenerA);
        pushConsumer02 = ConsumerFactory.getPushConsumer(account, topic, groupId02, new FilterExpression(tag), listenerB);
        pushConsumer03 = ConsumerFactory.getPushConsumer(account, topic, groupId03, new FilterExpression(tag), listenerC);

        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), listenerA.getDequeueMessages());
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), listenerB.getDequeueMessages());
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), listenerC.getDequeueMessages());
    }

    @Test
    @DisplayName("Send 100 normal messages synchronously, start 3 consumers on the same GroupId, expect 3 clients to consume a total of 100 messages")
    public void testClusterConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
//        String topic = "yueya-topic";
        String groupId = getGroupId(methodName);
        TestUtils.waitForSeconds(60);
        RMQNormalListener listenerA = new RMQNormalListener("ListenerA");
        RMQNormalListener listenerB = new RMQNormalListener("ListenerB");
        RMQNormalListener listenerC = new RMQNormalListener("ListenerC");
        pushConsumer01 = ConsumerFactory.getPushConsumer(account, topic, groupId, new FilterExpression(tag), listenerA);
        pushConsumer02 = ConsumerFactory.getPushConsumer(account, topic, groupId, new FilterExpression(tag), listenerB);
        pushConsumer03 = ConsumerFactory.getPushConsumer(account, topic, groupId, new FilterExpression(tag), listenerC);

        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyClusterConsume(producer.getEnqueueMessages(), listenerA.getDequeueMessages(), listenerB.getDequeueMessages(), listenerC.getDequeueMessages());
    }
}

