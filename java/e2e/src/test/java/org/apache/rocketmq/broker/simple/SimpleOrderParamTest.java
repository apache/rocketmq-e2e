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
import java.util.List;
import java.util.Map;
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
public class SimpleOrderParamTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(SimpleOrderParamTest.class);
    private String tag;
    private String groupId;
    private final static int SEND_NUM = 20;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
        groupId = NameUtils.getRandomGroupName();
        log.info("tag:{}, groupId:{}", tag, groupId);
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    @DisplayName("When sending 20 sequential messages synchronously using the same MessageGroup, SimpleConsumer normally receives () messages, but does not ack() messages, and keeps the sequence; the messages are stuck at the first")
    public void testFIFO_simple_receive_nack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        String groupId = getOrderlyGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        String messageGroup = RandomUtils.getStringByUUID();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), messageGroup);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        List<Runnable> runnables = new ArrayList<>(4);
        List<MessageView> receivedMessage = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            runnables.add(() -> {
                long startTime = System.currentTimeMillis();
                while (System.currentTimeMillis() < startTime + 30000) {
                    try {
                        List<MessageView> messageViews = consumer.receive(1, Duration.ofSeconds(10));
                        log.info("Get {} message: {}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                        for (MessageView messageView : messageViews) {
                            log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), messageView.getDeliveryAttempt());
                            receivedMessage.add(messageView);
                        }
                    } catch (ClientException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, 60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("A total of {} messages were received", receivedMessage.size());
        for (MessageView view : receivedMessage) {
            if (!StandardCharsets.UTF_8.decode(view.getBody()).toString().equals("0")) {
                Assertions.fail(String.format("Consumption out of order, expected :Body=%s Actual :Body=%s", 0, StandardCharsets.UTF_8.decode(view.getBody())));
            }
        }
    }

    @Disabled
    @DisplayName("20 sequential messages are sent synchronously, which are divided into eight Messagegroups. Normal receive() messages but no ack() messages are sent, and the sequence is maintained")
    public void testFIFO_simple_multiMessageGroup() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        String groupId = getOrderlyGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        try {
            consumer.receive(1, Duration.ofSeconds(10));
        } catch (ClientException e) {
            e.printStackTrace();
        }
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        String messageGroup = RandomUtils.getStringByUUID();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), messageGroup + String.valueOf(i % 8));
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        List<Runnable> runnables = new ArrayList<>(8);
        List<MessageView> receivedMessage = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() < startTime + 30000) {
                        try {
                            List<MessageView> messageViews = consumer.receive(1, Duration.ofSeconds(10));
                            log.info("Get {} message: {}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                            for (MessageView messageView : messageViews) {
                                log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Retry:{}", messageView.getMessageId(),
                                    StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), messageView.getDeliveryAttempt());
                                receivedMessage.add(messageView);
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
        log.info("A total of {} messages were received", receivedMessage.size());
        for (MessageView view : receivedMessage) {
            if (!StandardCharsets.UTF_8.decode(view.getBody()).toString().equals("0")) {
                Assertions.fail(String.format("Consumption out of order, expected :Body=%s Actual :Body=%s", 0, StandardCharsets.UTF_8.decode(view.getBody())));
            }
        }
    }

    @Test
    @DisplayName("Twenty sequential messages are sent synchronously and receive(3) in batch. All pulled messages are ack() messages except the first one. It is expected that all messages remain sequential and are consumed again after a certain time")
    public void testFIFO_simple_receive_multi_nack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        String groupId = getOrderlyGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");

        String messageGroup = RandomUtils.getStringByUUID();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), messageGroup);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        List<Runnable> runnables = new ArrayList<>(8);
        List<MessageView> receivedMessage = new ArrayList<>();
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    log.info("Start execution");
                    while (System.currentTimeMillis() < startTime + 30000) {
                        try {
                            List<MessageView> messageViews = consumer.receive(3, Duration.ofSeconds(10));
                            log.info("Get {} message: {}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                            for (int j = 0; j < messageViews.size(); j++) {
                                String messageId = messageViews.get(j).getMessageId().toString();
                                if (j > 0) {
                                    consumer.ack(messageViews.get(j));
                                    receivedMessage.add(messageViews.get(j));
                                    log.info("ack message:{}", messageId);
                                }
                                log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Retry:{}", messageId,
                                    StandardCharsets.UTF_8.decode(messageViews.get(j).getBody()), messageViews.get(j).getTag().get(), messageViews.get(j).getProperties(), messageViews.get(j).getDeliveryAttempt());
                                if (map.containsKey(messageId)) {
                                    map.put(messageId, map.get(messageId) + 1);
                                } else {
                                    map.put(messageId, 1);
                                }
                            }
                        } catch (ClientException e) {
                            log.info("exception");
                            e.printStackTrace();
                        }
                    }
                    log.info("End Execution");
                    Assertions.assertEquals(3, map.size());
                    for (Map.Entry<String, Integer> entry : map.entrySet()) {
                        Assertions.assertTrue(entry.getValue() > 1);
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, 60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("A total of {} messages were received", receivedMessage.size());
    }

    //TODO
    @Disabled
    @DisplayName("Send 20 sequential messages synchronously, batch receive(3), and only ack() the first message of pulled messages. Expect all messages to remain sequential and retry consumption after a certain time")
    public void testFIFO_simple_receive_multi_only_ack_first() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        String groupId = getOrderlyGroupId(methodName);

        SimpleConsumer consumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(consumer);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer, "Get Producer failed");
        String messageGroup = RandomUtils.getStringByUUID();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), messageGroup);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        List<Runnable> runnables = new ArrayList<>();
        ConcurrentHashMap<String, List<MessageView>> map = new ConcurrentHashMap<>();

        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    log.info("Start execution");
                    int consumeTimes = 0;
                    while (System.currentTimeMillis() < startTime + 30000) {
                        try {
                            List<MessageView> messageViews = consumer.receive(3, Duration.ofSeconds(10));
                            log.info("{} message obtained for {} time: {}", ++consumeTimes, messageViews.size(), Arrays.toString(messageViews.toArray()));
                            if (messageViews.size() > 0) {
                                Assertions.assertEquals(3, messageViews.size(), "The number of pulled messages does not meet expectations");
                            }
                            for (int j = 0; j < messageViews.size(); j++) {
                                List<MessageView> receivedMessage = new ArrayList<>();
                                String messageId = messageViews.get(j).getMessageId().toString();
                                String body = StandardCharsets.UTF_8.decode(messageViews.get(j).getBody()).toString();
                                if (j == 0) {
                                    consumer.ack(messageViews.get(j));
                                    log.info("ack message:{}, body:{}", messageId, StandardCharsets.UTF_8.decode(messageViews.get(j).getBody()));
                                }
                                if (map.containsKey(body)) {
                                    receivedMessage = map.get(body);
                                }
                                receivedMessage.add(messageViews.get(j));
                                map.put(StandardCharsets.UTF_8.decode(messageViews.get(j).getBody()).toString(), receivedMessage);
                                log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, DeliveryAttempt:{}", messageId,
                                    StandardCharsets.UTF_8.decode(messageViews.get(j).getBody()), messageViews.get(j).getTag().get(), messageViews.get(j).getProperties(), messageViews.get(j).getDeliveryAttempt());
                            }

                        } catch (ClientException e) {
                            log.info("exception==");
                            e.printStackTrace();
                        }
                    }
                    log.info("End Execution");
//                    Assertions.assertEquals(sendNum, receivedMessage.size(), "Consumption is not as expected");
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, 60);
            log.info("Start check");
            for (Map.Entry<String, List<MessageView>> entry : map.entrySet()) {
                if (entry.getKey().equals("0")) {
                    Assertions.assertEquals(1, entry.getValue().size(), "not expected");
                }
                if (entry.getKey().equals("1")) {
                    Assertions.assertTrue(entry.getValue().size() > 1);
                    List<MessageView> viewList = entry.getValue();
                    long actual = viewList.stream().filter(msg -> msg.getDeliveryAttempt() == 2).count();
                    Assertions.assertEquals(1, actual, String.format("The number of message retries obtained was not expected, expect:%s, actual:%s", 1, actual));
//                    DeliveryAttempt是从0开始计数的，判断重试应该让getDeliveryAttempt()==1, 一起拉取三条消息，第一条没重试，那重试的还剩两条，所以应该让actual==2
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

