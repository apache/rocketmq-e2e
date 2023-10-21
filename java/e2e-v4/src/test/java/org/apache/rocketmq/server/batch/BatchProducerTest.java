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

package org.apache.rocketmq.server.batch;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.cluster.LoadBalancingTest;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.BATCHPRODUCER)
public class BatchProducerTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(BatchProducerTest.class);
    private String tag;
    private final static int SEND_NUM = 10;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @Test
    @DisplayName("Send 10 messages in batch, expect pushconsumer to accept them all")
    public void testBatchProducer() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,
                new AllocateMessageQueueAveragely());
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        Assertions.assertNotNull(producer);
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            messages.add(message);
        }

        try {
            producer.getProducer().send(messages);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        TestUtils.waitForSeconds(5);

        Assertions.assertEquals(SEND_NUM, consumer.getListener().getDequeueMessages().getDataSize());

        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    @DisplayName("Send 10 messages to a queue in batch , expect pushconsumer to accept them all")
    public void testBatchProducer_queue() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,
                new AllocateMessageQueueAveragely());
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        Assertions.assertNotNull(producer);
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            messages.add(message);
        }

        List<MessageQueue> msgQueues = producer.fetchPublishMessageQueues(topic);
        try {
            producer.getProducer().send(messages, msgQueues.get(0));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        TestUtils.waitForSeconds(5);

        Assertions.assertEquals(SEND_NUM, consumer.getListener().getDequeueMessages().getDataSize());

        producer.shutdown();
        consumer.shutdown();
    }

}
