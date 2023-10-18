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

package org.apache.rocketmq.cluster;

import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.MODEL)
public class ClusterTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(ClusterTest.class);
    private String tag;
    private final static int SEND_NUM = 100;
    private RMQNormalConsumer pushConsumer01;
    private RMQNormalConsumer pushConsumer02;
    private RMQNormalConsumer pushConsumer03;
    private RMQNormalConsumer pullConsumer;
    private RMQNormalProducer producer;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @BeforeEach
    public void tearDown() {
        if (pushConsumer01 != null) {
            pushConsumer01.shutdown();
        }
        if (pushConsumer02 != null) {
            pushConsumer02.shutdown();
        }
        if (pushConsumer03 != null) {
            pushConsumer03.shutdown();
        }
        if (pullConsumer != null) {
            pullConsumer.shutdown();
        }
        if (producer != null) {
            producer.shutdown();
        }
    }

    @Test
    @DisplayName("Send 100 normal messages synchronously, start three consumers on different GroupId, and expect each client to consume up to 100 messages")
    public void testBroadcastConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId01 = getGroupId(methodName + 1);
        String groupId02 = getGroupId(methodName + 2);
        String groupId03 = getGroupId(methodName + 3);

        RMQNormalListener listenerA = new RMQNormalListener("ListenerA");
        RMQNormalListener listenerB = new RMQNormalListener("ListenerB");
        RMQNormalListener listenerC = new RMQNormalListener("ListenerC");
        pushConsumer01 = ConsumerFactory.getRMQBroadCastConsumer(namesrvAddr, groupId01, rpcHook);
        pushConsumer02 = ConsumerFactory.getRMQBroadCastConsumer(namesrvAddr, groupId02, rpcHook);
        pushConsumer03 = ConsumerFactory.getRMQBroadCastConsumer(namesrvAddr, groupId03, rpcHook);
        pushConsumer01.subscribeAndStart(topic,tag, listenerA);
        pushConsumer02.subscribeAndStart(topic,tag, listenerB);
        pushConsumer03.subscribeAndStart(topic,tag, listenerC);

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), listenerA.getDequeueMessages());
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), listenerB.getDequeueMessages());
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), listenerC.getDequeueMessages());
    }

    @Test
    @DisplayName("Send 100 normal messages synchronously, start three consumers on same GroupId, and expect each client to consume up to 100 messages")
    public void testBroadcastConsumeWithSameGroupId() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalListener listenerA = new RMQNormalListener("ListenerA");
        RMQNormalListener listenerB = new RMQNormalListener("ListenerB");
        RMQNormalListener listenerC = new RMQNormalListener("ListenerC");
        pushConsumer01 = ConsumerFactory.getRMQBroadCastConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer02 = ConsumerFactory.getRMQBroadCastConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer03 = ConsumerFactory.getRMQBroadCastConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer01.subscribeAndStart(topic,tag, listenerA);
        pushConsumer02.subscribeAndStart(topic,tag, listenerB);
        pushConsumer03.subscribeAndStart(topic,tag, listenerC);

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
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
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalListener listenerA = new RMQNormalListener("ListenerA");
        RMQNormalListener listenerB = new RMQNormalListener("ListenerB");
        RMQNormalListener listenerC = new RMQNormalListener("ListenerC");
        pushConsumer01 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer02 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer03 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer01.subscribeAndStart(topic,tag, listenerA);
        pushConsumer02.subscribeAndStart(topic,tag, listenerB);
        pushConsumer03.subscribeAndStart(topic,tag, listenerC);

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = new Message(topic, tag, String.valueOf(i).getBytes());
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyClusterConsume(producer.getEnqueueMessages(), listenerA.getDequeueMessages(), listenerB.getDequeueMessages(), listenerC.getDequeueMessages());
    }
}

