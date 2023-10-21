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

package org.apache.rocketmq.server.delay;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.SMOKE)
@Tag(TESTSET.DELAY)
public class DelayMessageTest extends BaseOperate {
    private final Logger logger = LoggerFactory.getLogger(DelayMessageTest.class);
    // private RMQOrderConsumer consumer;
    private String tag;
    private String topic;
    private String groupId;
    private final static int SEND_NUM = 10;

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
    @DisplayName("Send 10 delay messages and set the delay test delay level=1 , expecting all to be consumed and latency is as expected")
    public void testDelayLevel1() {
        int delayLevel = 1;
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        consumer.subscribeAndStart(topic, "*", new RMQNormalListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        producer.sendDelay(topic, delayLevel, SEND_NUM);
        VerifyUtils.verifyDelayMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages(),
                delayLevel);

        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    @DisplayName("Send 10 delay messages and set the delay test delay level=4 , expecting all to be consumed and latency is as expected")
    public void testDelayLevel4() {
        int delayLevel = 4;
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        consumer.subscribeAndStart(topic, "*", new RMQNormalListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        producer.sendDelay(topic, delayLevel, SEND_NUM);
        VerifyUtils.verifyDelayMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages(),
                delayLevel);

        producer.shutdown();
        consumer.shutdown();
    }

    @Disabled
    @Test
    @DisplayName("Send one delay message and set the delay test negative delay level, expecting message building wrong")
    public void testNegativeDelayLevel() {
        int delayLevel = -1;
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        assertThrows(Exception.class, () -> {
            Message msg = new Message(topic, "*", RandomUtils.getStringByUUID().getBytes());
            msg.setDelayTimeLevel(delayLevel);
            SendResult sendResult = producer.getProducer().send(msg);
            logger.info(sendResult.toString());
        }, "Send messages with a negative delay level, Expected send() to throw exception, but it didn't");

        producer.shutdown();
    }

    @Disabled
    @Test
    @DisplayName("Send one delay message and set the delay test delay level=19, expecting message building wrong")
    public void testDelayLevelWith19() {
        int delayLevel = 19;
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        assertThrows(Exception.class, () -> {
            Message msg = new Message(topic, "*", RandomUtils.getStringByUUID().getBytes());
            msg.setDelayTimeLevel(delayLevel);
            SendResult sendResult = producer.getProducer().send(msg);
            logger.info(sendResult.toString());
        }, "Send messages with delay level=19, Expected send() to throw exception, but it didn't");

        producer.shutdown();
    }
}
