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

import java.util.concurrent.Callable;

import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Test message body
 */
@Tag(TESTSET.CLIENT)
public class MessageBodyContentTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(MessageBodyContentTest.class);
    private static String topic;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer pullConsumer;

    @BeforeAll
    public static void setUpAll() {
        topic = getTopic("MessageBodyContentTest");
    }

    @BeforeEach
    public void setUp() {
        
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

    @Test
    @DisplayName("Send normal message, setting message body with space character, expect consume success")
    public void testMessageBodyContentIsSpace() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        String tag = NameUtils.getRandomTagName();

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        String body = " ";
        producer.send(topic, tag, body);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        await().atMost(60, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return pushConsumer.getListener().getDequeueMessages().getDataSize() == 1;
            }
        });

        VerifyUtils.verifyNormalMessageWithBody(producer.getEnqueueMessages(),
                pushConsumer.getListener().getDequeueMessages(), body);
    }

    @Test
    @DisplayName("Send normal message, setting message body with chinese character, expect consume success")
    public void testMessageBodyContentIsChinese() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        String tag = NameUtils.getRandomTagName();

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        String body = "中文字符";
        producer.send(topic, tag, body);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(60, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return pushConsumer.getListener().getDequeueMessages().getDataSize() == 1;
            }
        });

        VerifyUtils.verifyNormalMessageWithBody(producer.getEnqueueMessages(),
                pushConsumer.getListener().getDequeueMessages(), body);
    }

    @Test
    @DisplayName("Send normal message, setting message body with emoji(😱) character, expect consume success ")
    public void testMessageBodyContentIsEmoji() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        String tag = NameUtils.getRandomTagName();

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        String body = "😱";
        producer.send(topic, tag, body);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(60, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return pushConsumer.getListener().getDequeueMessages().getDataSize() == 1;
            }
        });

        VerifyUtils.verifyNormalMessageWithBody(producer.getEnqueueMessages(),
                pushConsumer.getListener().getDequeueMessages(), body);
    }
}
