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

package org.apache.rocketmq.broker.client.message;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test message key
 */
@Tag(TESTSET.CLIENT)
public class MessageKeyTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(MessageKeyTest.class);
    private static String topic;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer simpleConsumer;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
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
    @DisplayName("Message Key beyond 16KB, expect throw exception")
    public void testMessageKeyBeyond16KB() {
        producer = ProducerFactory.getRMQProducer(account, topic);
        String body = RandomStringUtils.randomAlphabetic(64);
        String key = RandomStringUtils.randomAlphabetic(16 * 1024 + 1);

        Assertions.assertNotNull(producer);
        assertThrows(Exception.class, () -> {
            Message message = provider.newMessageBuilder()
                .setTopic(topic)
                .setKeys(key)
                .setBody(body.getBytes(StandardCharsets.UTF_8))
                .build();
            producer.getProducer().send(message);
        }, " message key beyond 16KB , expect throw exception but it didn't");
    }

    @Test
    @DisplayName("Message Key equals 16KB, expect send success")
    public void testMessageKeyEquals16KB() {
        producer = ProducerFactory.getRMQProducer(account, topic);
        String body = RandomStringUtils.randomAlphabetic(64);
        String key = RandomStringUtils.randomAlphabetic(16 * 1024);
        Assertions.assertNotNull(producer);

        Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setKeys(key)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .build();
        producer.send(message);
    }

    @Test
    @DisplayName("Message Key contains invisible characters \u0000 ,expect throw exception")
    public void testMessageKeyWithInvisibleCharacter() {
        producer = ProducerFactory.getRMQProducer(account, topic);
        String body = RandomStringUtils.randomAlphabetic(64);
        String key = "\u0000";

        Assertions.assertNotNull(producer);
        assertThrows(Exception.class, () -> {
            Message message = provider.newMessageBuilder()
                .setTopic(topic)
                .setKeys(key)
                .setBody(body.getBytes(StandardCharsets.UTF_8))
                .build();
            producer.getProducer().send(message);
        }, " message key contains invisible character ,expect throw exception but it didn't");
    }

    @Test
    @DisplayName("Message key contains Chinese, expect send and consume success")
    public void testMessageKeyContentWithChinese() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);

        String key = "中文字符";
        String tag = NameUtils.getRandomTagName();
        String body = RandomStringUtils.randomAlphabetic(64);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

        Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setKeys(key)
            .setTag(tag)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .build();
        producer.send(message);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("The message contains multiple keys, expect send and consume success")
    public void testMessageWithMultiKey() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);

        String key1 = RandomStringUtils.randomAlphabetic(64);
        String key2 = RandomStringUtils.randomAlphabetic(64);
        String tag = NameUtils.getRandomTagName();
        String body = RandomStringUtils.randomAlphabetic(64);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

        Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setKeys(key1, key2)
            .setTag(tag)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .build();
        producer.send(message);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }
}


