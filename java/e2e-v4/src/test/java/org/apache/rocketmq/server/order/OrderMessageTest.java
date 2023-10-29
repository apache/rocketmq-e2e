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

package org.apache.rocketmq.server.order;

import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQOrderListener;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Tag(TESTSET.SMOKE)
@Tag(TESTSET.ORDER)
public class OrderMessageTest extends BaseOperate {
    private final Logger logger = LoggerFactory.getLogger(OrderMessageTest.class);
    private final static int SEND_NUM = 100;
    private static String topic = getTopic("OrderMessageTest");

    @AfterEach
    public void tearDown() {

    }

    @AfterAll
    public static void tearDownAll() {
        MQAdmin.deleteTopic(namesrvAddr, cluster , topic);
    }

    @Test
    @DisplayName("Thirty messages are sent to each of the eight queues in a topic, with the expectation that the sequential consumption client will consume the messages in each queue in order")
    public void testConsumePartitionOrderMessage() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        consumer.subscribeAndStart(topic, tag, new RMQOrderListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(topic);
        producer.sendWithQueue(messageQueues, tag, 30);
        VerifyUtils.verifyOrderMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());

        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    @DisplayName("100 messages are sent to a queue for a topic, with the expectation that the sequential consuming client will consume the messages in the queue in order")
    public void testConsumeGlobalOrderMessage() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        consumer.subscribeAndStart(topic, tag, new RMQOrderListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(topic);
        messageQueues.removeIf(messageQueue -> messageQueue.getQueueId() != 0);
        producer.sendWithQueue(messageQueues, tag, SEND_NUM);
        VerifyUtils.verifyOrderMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());

        producer.shutdown();
        consumer.shutdown();
    }
}
