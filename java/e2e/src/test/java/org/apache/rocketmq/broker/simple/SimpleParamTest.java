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

package org.apache.rocketmq.broker.simple;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.util.AssertUtils;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.RandomUtils;
import org.apache.rocketmq.util.TestUtils;
import org.apache.rocketmq.util.VerifyUtils;
import org.apache.rocketmq.util.data.collect.DataCollector;
import org.apache.rocketmq.util.data.collect.DataCollectorManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.SIMPLE)
public class SimpleParamTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(SimpleOrderTest.class);
    private String tag;
    private String groupId;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
        groupId = NameUtils.getRandomGroupName();
        log.info("tag:{}, groupId:{}", tag, groupId);
    }

    @AfterEach
    public void tearDown() {
    }

//    @Test
//    @DisplayName("If you use SimpleConsumer to set the maximum number of retries of GroupId to 3, the maximum number of retries is expected to be 3, and the retry interval is equal to 10s of the received parameter")
//    public void testNormal_simple_maxRetryTimes() {
//        String className = this.getClass().getName();
//        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
//
//        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
//        String groupId = getGroupId(methodName);
//
//        groupWrapper.updateConsumerGroup(primaryAccount1, account1InstanceId, groupId, null, "Concurrently", "DefaultRetryPolicy", 1, true);
//
//        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(primaryAccount1, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
//        VerifyUtils.tryReceiveOnce(consumer);
//        RMQNormalProducer producer = ProducerFactory.getRMQProducer(primaryAccount1, topic);
//        Assertions.assertNotNull(producer, "Get Producer failed");
//        Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
//        producer.send(message);
//        VerifyUtils.waitReceiveThenAck(producer, consumer, 1, Duration.ofSeconds(10), 2, 40, false);
//    }

    //TODO
    @Disabled
    @DisplayName("After the SimpleConsumer receive(1,10s) message is used, an ack message is performed after three retries. Three retries are expected and the retry interval is equal to 10s of the receive parameter transmission")
    public void testNormal_simple_receive10_ack_retry3() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
        producer.send(message);
        VerifyUtils.waitReceiveThenAck(producer, consumer, 1, Duration.ofSeconds(10), 4);
    }

    @Test
    @DisplayName("Send an ordinary message synchronously. After using SimpleConsumer receive(1,10s) message, perform an ack in the 11s section. If an INVALID_RECEIPT_HANDLE error message is displayed, then receive a new message again. And successfully ack")
    public void test_waitAckException_reReceive_ack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(15));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
        producer.send(message);
        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");

        try {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTime + 60000) {
                List<MessageView> messageViews = consumer.receive(1, Duration.ofSeconds(10));
                if (messageViews.size() > 0) {
                    for (MessageView messageView : messageViews) {
                        log.info("MessageId:{}, Body:{}, tag:{}, property:{}", messageView.getMessageId(), StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties());
                        TestUtils.waitForSeconds(11);
                        consumer.ack(messageView);
                        Assertions.fail("Calling changeInvisibleDuration after ack fails with an INVALID_RECEIPT_HANDLE error");
                    }
                } else {
                    log.info("receive 0 messages");
                }
            }
            Assertions.fail("Expect consumption to be abnormal, but it is not");
        } catch (ClientException e) {
            e.printStackTrace();
            Assertions.assertTrue(e.getMessage().contains("INVALID_RECEIPT_HANDLE"), e.getMessage());
        }

        VerifyUtils.waitReceiveThenAck(producer, consumer, 1, Duration.ofSeconds(30));
    }

    //TODO
    @Disabled
    @DisplayName("After the SimpleConsumer receive(1,10s) message is used, the message is successfully consumed for the first time, the invisible time of the message is successfully delayed to 20s, the Receive is continued, and the message is expected to be pulled and ack successfully in the 20s")
    public void test_nack_changeInvisibleDuration20() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
        producer.send(message);
        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");

        VerifyUtils.waitReceiveThenNack(producer, consumer, 1, Duration.ofSeconds(10), Duration.ofSeconds(20));
        long firstReceivedTime = System.currentTimeMillis();

        final boolean[] hasReceived = {false};
        List<Runnable> runnables = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    long nextReceivedTime = 0L;
                    while (System.currentTimeMillis() < startTime + 60000 && !hasReceived[0]) {
                        try {
                            List<MessageView> messageViews = consumer.receive(1, Duration.ofSeconds(30));
                            log.info("Get {} message: {}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                            for (MessageView messageView : messageViews) {
                                log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Retry:{}", messageView.getMessageId(),
                                    StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), messageView.getDeliveryAttempt());
                                consumer.ack(messageView);
                                hasReceived[0] = true;
                                nextReceivedTime = System.currentTimeMillis();
                                String logInfo = String.format("receive return time exception, firstReceivedTime:%s, nextReceivedTime:%s, intervalTime:%s", firstReceivedTime, nextReceivedTime, nextReceivedTime - firstReceivedTime);
                                log.info(logInfo);
                                Assertions.assertTrue((nextReceivedTime - firstReceivedTime) > 20000 && (nextReceivedTime - firstReceivedTime) <= 30000, logInfo + ",the expectation is greater than 20,000 and less than or equal to 30,000");
                            }
                        } catch (ClientException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, 60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Disabled
    @DisplayName("Send a normal message synchronously, after using the SimpleConsumer receive(1,10s) message, after consuming the message, ack and changeInvisibleDuration, expecting change indicating illegal ReceiptHandle")
    public void testNormal_simple_receive_ack_changeInvisibleDuration() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(15));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
        producer.send(message);
        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");

        try {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTime + 60000) {
                List<MessageView> messageViews = consumer.receive(1, Duration.ofSeconds(10));
                if (messageViews.size() > 0) {
                    for (MessageView messageView : messageViews) {
                        log.info("MessageId:{}, Body:{}, tag:{}, property:{}", messageView.getMessageId(), StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties());
                        consumer.ack(messageView);
                        consumer.changeInvisibleDuration(messageView, Duration.ofSeconds(10));
                        Assertions.fail("Calling changeInvisibleDuration after ack was expected to fail, but succeeded");
                    }
                } else {
                    log.info("receive 0 messages");
                }
            }
            Assertions.fail("Expect consumption to be abnormal, but it is not");
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Send 300 normal messages synchronously, and after using SimpleConsumer receive(30,10s) messages, ack them after consuming them, expecting each receive to be less than or equal to 32 messages, and never receive the ack messages again")
    public void testNormal_simple_receive_maxsize_sync() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        int sendNum = 300;
        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(15));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        for (int i = 0; i < sendNum; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.send(message);
        }
        Assertions.assertEquals(sendNum, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Map<String, MessageView> recvMsgs = new ConcurrentHashMap<>();
        try {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTime + 90000) {
                List<MessageView> messageViews = consumer.receive(50, Duration.ofSeconds(10));
                if (messageViews.size() > 0) {
                    Assertions.assertTrue(messageViews.size() <= 32);
                    for (MessageView messageView : messageViews) {
                        log.info("MessageId:{}, Body:{}, tag:{}, property:{}", messageView.getMessageId(), StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties());
                        if (recvMsgs.containsKey(messageView.getMessageId().toString())) {
                            Assertions.fail("Consume an ack message");
                        } else {
                            consumer.ack(messageView);
                            recvMsgs.put(messageView.getMessageId().toString(), messageView);
                        }
                    }
                }
                log.info("receive {} A message", messageViews.size());
                if (recvMsgs.size() == sendNum) {
                    break;
                }
            }
            DataCollector dequeueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
            for (MessageView messageView : recvMsgs.values()) {
                dequeueMessages.addData(messageView);
            }
            VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), dequeueMessages, 10);
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Twenty ordinary messages are sent synchronously, and receive(50) messages are received in batch. All the pulled messages are ack() messages except the first one. expected the ack messages will not be consumed repeatedly and subsequent consumption will not be affected")
    public void testNormal_simple_receive_multi_nack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        int sendNum = 20;
        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        for (int i = 0; i < sendNum; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.send(message);
        }
        Assertions.assertEquals(sendNum, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Map<String, MessageView> recvMsgs = new ConcurrentHashMap<>();
        List<Runnable> runnables = new ArrayList<>(8);

        long startTime = System.currentTimeMillis();
        final boolean[] flag = {true};
        Set<String> unconsumedMsgIds = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    while (System.currentTimeMillis() < startTime + 40000) {
                        List<MessageView> messageViews = null;
                        try {
                            messageViews = consumer.receive(50, Duration.ofSeconds(10));
                            if (messageViews.size() > 0) {
                                Assertions.assertTrue(messageViews.size() <= 32);
                                for (MessageView messageView : messageViews) {
                                    log.info("MessageId:{}, Body:{}, tag:{}, deliveryAttempt:{}, property:{}", messageView.getMessageId(), StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getDeliveryAttempt(), messageView.getProperties());
                                    if (flag[0]) {
                                        flag[0] = false;
                                        unconsumedMsgIds.add(messageView.getMessageId().toString());
                                        log.info("nack message:{}", messageView);
                                    } else {
                                        if (messageView.getDeliveryAttempt() == 1) {
                                            consumer.ack(messageView);
                                            if (recvMsgs.containsKey(messageView.getMessageId().toString())) {
                                                Assertions.fail("Consume an ack message");
                                            } else {
                                                recvMsgs.put(messageView.getMessageId().toString(), messageView);
                                            }
                                        } else {
                                            unconsumedMsgIds.add(messageView.getMessageId().toString());
                                        }
                                    }
                                }
                            }
                        } catch (ClientException e) {
                            e.printStackTrace();
                        }
                        log.info("receive {} messages", messageViews.size());
                        if (recvMsgs.size() == sendNum) {
                            break;
                        }
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrentAtMost("Test Failed", runnables, 40);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DataCollector dequeueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        for (MessageView messageView : recvMsgs.values()) {
            dequeueMessages.addData(messageView);
        }
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), dequeueMessages, unconsumedMsgIds, 10);

    }

}

