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

package org.apache.rocketmq.client.message;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test message tag
 */
@Tag(TESTSET.CLIENT)
public class MessageTagTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(MessageTagTest.class);
    private static String topic;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer pullConsumer;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(methodName);
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.shutdown();
        }
        if (pushConsumer != null) {
            pushConsumer.shutdown();
        }
        if (pullConsumer != null) {
            pullConsumer.shutdown();
        }
    }

    @Disabled
    @DisplayName("Message Tag beyond 16KB, expect throw exception")
    public void testMessageTagBeyond16KB() {
        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        String tag = RandomStringUtils.randomAlphabetic(16 * 1024 + 1);
        String body = RandomStringUtils.randomAlphabetic(64);

        Assertions.assertNotNull(producer);
        assertThrows(Exception.class, () -> {
            Message message = new Message(topic, tag, RandomUtils.getStringByUUID(), body.getBytes());
            producer.getProducer().send(message);
        }, " message tag beyond 16KB ,expect throw exception but it didn't");
    }

    @Disabled
    @DisplayName("Message Tag equals 16KB, expect send success")
    public void testMessageTagEquals16KB() {
        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        String tag = RandomStringUtils.randomAlphabetic(16 * 1024);
        String body = RandomStringUtils.randomAlphabetic(64);

        Message message = new Message(topic, tag, RandomUtils.getStringByUUID(), body.getBytes());
        producer.send(message);
    }

    @Disabled
    @DisplayName("Message Tag contains invisible characters \u0000 ,expect throw exception")
    public void testMessageTagWithInvisibleCharacter() {
        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        String tag = "\u0000";

        Assertions.assertNotNull(producer);
        assertThrows(Exception.class, () -> {
            Message message = new Message(topic, tag, RandomUtils.getStringByUUID(),
                    RandomUtils.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            producer.getProducer().send(message);
        }, " message tag contains invisible character ,expect throw exception but it didn't");
    }

    @Disabled
    @DisplayName("Message Tag contains | , expect throw exception")
    public void testMessageTagContentWith() {
        String tag = "tag|";
        String body = RandomStringUtils.randomAlphabetic(64);

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        assertThrows(Exception.class, () -> {
            Message message = new Message(topic, tag, RandomUtils.getStringByUUID(),
                    body.getBytes(StandardCharsets.UTF_8));
            producer.getProducer().send(message);
        }, " message tag contains | , expect throw exception but it didn't");

    }

    @Test
    @DisplayName("Message Tag contains Chinese, expect send and consume success")
    public void testMessageTagContentWithChinese() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);

        String tag = "中文字符";
        String body = RandomStringUtils.randomAlphabetic(64);

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        pullConsumer = ConsumerFactory.getRMQLitePullConsumer(namesrvAddr, groupId, rpcHook);
        pullConsumer.subscribeAndStartLitePull(topic, tag);
        VerifyUtils.tryReceiveOnce(pullConsumer.getLitePullConsumer());
        pullConsumer.shutdown();

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer);

        Message message = new Message(topic, tag, RandomUtils.getStringByUUID(), body.getBytes(StandardCharsets.UTF_8));
        producer.send(message);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }
}
