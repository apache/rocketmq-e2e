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

package com.apache.rocketmq.server.order;

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
    private String tag;
    private String topic;
    private String groupId;
    private final static int SEND_NUM = 100;

    @BeforeEach
    public void setUp() {
        topic = NameUtils.getTopicName();
        tag = NameUtils.getTagName();
        groupId = NameUtils.getGroupName();
        MQAdmin.createTopic(namesrvAddr, cluster, topic, 8);
        logger.info("topic:{}, tag:{}, groupId:{}", topic, tag, groupId);
    }

    @AfterEach
    public void tearDown() {

    }

    @Test
    @DisplayName("使用分区顺序topic，设置8个shardingkey，发送100条分区顺序消息，期望能按顺序消费到全部消息")
    public void testConsumePartitionOrderMessage() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, "*", new RMQOrderListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr);
        List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(topic);
        producer.sendWithQueue(messageQueues, SEND_NUM);
        VerifyUtils.verifyOrderMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());

        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    @DisplayName("使用全局顺序topic，发送100条全局顺序消息，期望能按顺序消费到全部消息")
    public void testConsumeGlobalOrderMessage() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, "*", new RMQOrderListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr);
        List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(topic);
        messageQueues.removeIf(messageQueue -> messageQueue.getQueueId() != 0);
        producer.sendWithQueue(messageQueues, SEND_NUM);
        VerifyUtils.verifyOrderMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());

        producer.shutdown();
        consumer.shutdown();
    }
}
