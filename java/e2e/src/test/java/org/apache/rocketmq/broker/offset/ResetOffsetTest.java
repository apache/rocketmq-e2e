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

package org.apache.rocketmq.broker.offset;

import java.time.Duration;
import java.util.Collection;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
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
import org.apache.rocketmq.util.data.collect.DataCollector;
import org.apache.rocketmq.util.data.collect.DataCollectorManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.NORMAL)
public class ResetOffsetTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(ResetOffsetTest.class);
    private String tag;
    private final static int SEND_NUM = 10;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer simpleConsumer;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (pushConsumer != null) {
            pushConsumer.close();
        }
        if (simpleConsumer != null) {
            simpleConsumer.close();
        }
    }

    @Test
    @DisplayName("Send 10 messages, reset the offset to the beginning after successful consumption, and expect to consume the 10 messages again")
    public void testResetOffsetFromBeginning() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        long timestamp = System.currentTimeMillis();
        TestUtils.waitForSeconds(1);
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        DataCollector enqueueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        Collection<Object> collection = producer.getEnqueueMessages().getAllData();
        for (Object obj : collection) {
            enqueueMessages.addData(obj);
        }

        VerifyUtils.verifyNormalMessage(enqueueMessages, pushConsumer.getListener().getDequeueMessages());
        pushConsumer.getListener().getDequeueMessages().resetData();

        //reset offset from begin
        resetOffsetByTimestamp(groupId, topic, timestamp);

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());

        pushConsumer.close();
        producer.close();
    }

    @Test
    @DisplayName("Send 10 messages, reset the offset to the latest after successful consumption, and expect to consume the 10 messages again")
    public void testResetOffsetFromLatest() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        for (int i = 0; i < 30; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(30, producer.getEnqueueMessages().getDataSize(), "send message failed");

        TestUtils.waitForSeconds(10);
        resetOffsetByTimestamp(groupId, topic, System.currentTimeMillis());

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        VerifyUtils.waitForConsumeFailed(pushConsumer.getListener().getDequeueMessages(), 30);

        pushConsumer.close();
        producer.close();
    }
}

