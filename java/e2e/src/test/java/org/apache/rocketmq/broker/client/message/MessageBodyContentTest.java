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

import java.time.Duration;
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
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test message body
 */
@Tag(TESTSET.CLIENT)
public class MessageBodyContentTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(MessageBodyContentTest.class);
    private String tag;
    private static String topic;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer simpleConsumer;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
    }

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
    @DisplayName("Send normal message, setting message body with space character, expect consume success")
    public void testMessageBodyContentIsSpace() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);

        String body = " ";
        Message message = MessageFactory.buildMessage(topic, tag, body);
        assert producer != null;
        producer.send(message);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessageWithBody(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), body);
    }

    @Test
    @DisplayName("Send normal message, setting message body with chinese character, expect consume success")
    public void testMessageBodyContentIsChinese() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);

        String body = "中文字符";
        Message message = MessageFactory.buildMessage(topic, tag, body);
        assert producer != null;
        producer.send(message);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessageWithBody(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), body);
    }

    @Test
    @DisplayName("Send normal message, setting message body with emoji(😱) character, expect consume success ")
    public void testMessageBodyContentIsEmoji() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);

        String body = "😱";
        Message message = MessageFactory.buildMessage(topic, tag, body);
        assert producer != null;
        producer.send(message);

        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessageWithBody(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), body);
    }
}


