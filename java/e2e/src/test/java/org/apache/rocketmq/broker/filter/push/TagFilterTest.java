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

package org.apache.rocketmq.broker.filter.push;

import java.time.Duration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.TestUtils;
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisplayName("TAG filtering test: Use PushConsumer for consumption")
@Tag(TESTSET.TAG)
public class TagFilterTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(TagFilterTest.class);
    private final static int SEND_NUM = 10;
    RMQNormalProducer producer;
    RMQNormalConsumer pushConsumer;
    RMQNormalConsumer simpleConsumer;

    @BeforeEach
    public void setUp() {

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
    @DisplayName("Using tagA sent 10 messages, the use of tagA | | tagB filter messages, expect consumption to send 10 messages")
    public void testSendTagA_SubTagAorTagB() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTag = NameUtils.getRandomTagName();
        String receiveTag = sendTag + "||TagB";
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());
        log.info("Wait for the SimpleConsumer");
        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTag, SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Use tagA sent 10 messages first, after using tagB sent 10 messages, use tagA | | tagB filter messages, expect consumption to send 20 messages")
    public void testSndTagATagB_SubTagATagB() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = NameUtils.getRandomTagName();
        String sendTagB = NameUtils.getRandomTagName();
        String receiveTag = sendTagA + "||" + sendTagB;
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());
        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);
        producer.send(topic, sendTagB, SEND_NUM);

        Assertions.assertEquals(SEND_NUM * 2, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("The tagA is used to send 10 messages, then the tagB is used to send 10 messages, and the * is used to filter the messages, expecting to consume 20 messages sent")
    public void testSendTagAAndTagB_SubAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = NameUtils.getRandomTagName();
        String sendTagB = NameUtils.getRandomTagName();
        String receiveTag = "*";
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);
        producer.send(topic, sendTagB, SEND_NUM);

        Assertions.assertEquals(SEND_NUM * 2, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Send 10 tagA messages, subscribe to tagB messages, expect to consume up to 0 messages")
    public void testSendTagA_SubTagB() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = NameUtils.getRandomTagName();
        String receiveTag = NameUtils.getRandomTagName();
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        TestUtils.waitForSeconds(20);
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
    }

    @Test
    @DisplayName("Send 10 tagA messages, subscribe to tagA messages, expect to consume up to 10 messages")
    public void testSendTagA_SubTagA() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTag = NameUtils.getRandomTagName();
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(sendTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(sendTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTag, SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Consumption uses a very long tagA, sending 10 messages, expecting to consume 10 tagA messages")
    public void testLongTagSize() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTag = RandomStringUtils.randomAlphanumeric(1024 * 10);
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(sendTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(sendTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTag, SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("The consumption uses a space-spaced tag, and two tags are used to send 10 messages each, with the expectation of consuming up to 20 messages")
    public void testSubTagWithSpace() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = NameUtils.getRandomTagName();
        String sendTagB = NameUtils.getRandomTagName();
        String receiveTag = " " + sendTagA + " || " + sendTagB + " ";
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);
        producer.send(topic, sendTagB, SEND_NUM);

        Assertions.assertEquals(SEND_NUM * 2, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Send 10 tag = '@ | | | @' news, expect to send an exception is thrown, the tag is not allowed to include |")
    public void testTagWithSpecialSymbol01() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

        Assertions.assertThrows(Exception.class, () -> {
            producer.send(topic, "|@", SEND_NUM);
        }, "Send messages with  tag \"|@\", Expected send() to throw exception, but it didn't");
    }

    @Test
    @DisplayName("Send 10 messages with tag='*', subscribe to messages with tag='*', expect to consume the message")
    public void testTagWithSpecialSymbol02() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression("*"), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression("*"), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, "*", SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Consumer use | | separators between the tag, respectively using two tag each 10 messages sent, and expect consumption to 20 messages")
    public void testTagWithSpecialSymbol03() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = NameUtils.getRandomTagName();
        String sendTagB = NameUtils.getRandomTagName();
        String receiveTag = sendTagA + "||||" + sendTagB;
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);
        producer.send(topic, sendTagB, SEND_NUM);

        Assertions.assertEquals(SEND_NUM * 2, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
        pushConsumer.close();
        producer.close();
    }

    @Test
    @DisplayName("Send 10 messages each using the whitespace characters tag\"\" and \"\", expecting the send to throw an exception")
    public void testTagWithBlankSymbol() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);

        String sendTagA = "";
        String sendTagB = " ";

        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        Assertions.assertThrows(Exception.class, () -> {
            producer.send(topic, sendTagA, SEND_NUM);
        }, "Send messages with blank tag \"\", Expected send() to throw exception, but it didn't");
        Assertions.assertThrows(Exception.class, () -> {
            producer.send(topic, sendTagB, SEND_NUM);
        }, "Send messages with blank tag \" \", Expected send() to throw exception, but it didn't");
    }

    @Test
    @DisplayName("The sent tag uses two strings with the same hash value, and the consumed tag uses BB, expecting to consume messages with tag=BB")
    public void testSendTagWithSameHashCode_SubWithOne() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = "BB";
        String sendTagB = "Aa";
        String receiveTag = "BB";
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
        pushConsumer.getListener().clearMsg();
        producer.send(topic, sendTagB, SEND_NUM);
        TestUtils.waitForSeconds(10);
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueAllMessages().getDataSize());
    }

    @Test
    @DisplayName("Send 10 messages with tag=BB, 10 messages with tag=bb, subscribe with tag=BB, expect case-sensitive messages to be consumed to tag=BB")
    public void testTagCaseSensitive() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        String sendTagA = "BB";
        String sendTagB = "bb";
        String receiveTag = "BB";
        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(receiveTag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(receiveTag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        producer.send(topic, sendTagA, SEND_NUM);
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
        pushConsumer.getListener().clearMsg();
        producer.send(topic, sendTagB, SEND_NUM);
        TestUtils.waitForSeconds(10);
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueAllMessages().getDataSize());
    }
}

