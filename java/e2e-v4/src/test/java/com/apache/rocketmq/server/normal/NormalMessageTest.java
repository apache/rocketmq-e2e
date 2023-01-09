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

package com.apache.rocketmq.server.normal;

import org.apache.rocketmq.client.callback.RMQSendCallBack;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.NORMAL)
@Tag(TESTSET.SMOKE)
public class NormalMessageTest extends BaseOperate {
    private final Logger logger = LoggerFactory.getLogger(NormalMessageTest.class);
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

    @Test
    @DisplayName("同步发送10条普通消息，期望这10条消息被消费到")
    public void testConsumeNormalMessage() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr);
        producer.send(topic, tag, SEND_NUM);

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());
        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    @DisplayName("异步发送10条普通消息，期望这10条消息被消费到")
    public void testConsumeNormalMessageAndSendWithAsync() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr);
        RMQSendCallBack callBack = new RMQSendCallBack();
        producer.sendAsync(topic, tag, callBack, SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "消息发送失败");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("OneWay发送10条普通消息，期望这10条消息被消费到")
    public void testConsumeNormalMessageAndSendWithOneWay() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr);

        producer.sendOneWay(topic, tag, SEND_NUM);

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("topic创建,然后发送消息,不消费,删除该topic再次创建同名topic,启动消费,预期无法再次消费到")
    public void testConsumeNormalMessageWithDeleteAndCreateTopicAgain() {

        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr);
        producer.send(topic, tag, SEND_NUM);

        MQAdmin.deleteTopic(namesrvAddr, cluster, topic);

        MQAdmin.createTopic(namesrvAddr, cluster, topic, 8);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());
        VerifyUtils.waitForConsumeFailed(consumer.getListener().getDequeueMessages(), 20);

        producer.shutdown();
        consumer.shutdown();
    }

}

