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

package org.apache.rocketmq.broker.server;

import java.time.Duration;
import java.util.concurrent.Callable;
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
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Tag(TESTSET.DELAY)
@Tag(TESTSET.SMOKE)
public class DelayMessageTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(DelayMessageTest.class);
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
    @DisplayName("Send 10 messages timed 10 seconds later synchronously, and expect these 10 messages to be consumed by PushConsumer 10 seconds later")
    public void testDelay_Send_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.DELAY.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildDelayMessage(topic, tag, RandomUtils.getStringByUUID(), System.currentTimeMillis() + 10 * 1000);
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyDelayMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), 15);
    }

    @Test
    @DisplayName("10 messages are sent asynchronously and are expected to be consumed by PushConsumer 10 seconds later")
    public void testDelay_SendAsync_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.DELAY.getValue(), methodName);
        String groupId = getGroupId(methodName);
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildDelayMessage(topic, tag, RandomUtils.getStringByUUID(), System.currentTimeMillis() + 10 * 1000);
            producer.sendAsync(message);
        }
        await().atMost(10, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return SEND_NUM == producer.getEnqueueMessages().getDataSize();
            }
        });
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyDelayMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), 15);
    }

    @Test
    @DisplayName("Sends 10 timed messages (-20s) expecting to be delivered and consumed immediately")
    public void testDelayTime15SecondsAgo() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.DELAY.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildDelayMessage(topic, tag, RandomUtils.getStringByUUID(), System.currentTimeMillis() - 20 * 1000);
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyDelayMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), 0);
    }

}

