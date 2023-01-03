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

package org.apache.rocketmq.broker.server.abnormal;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ClientConfigurationFactory;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.RandomUtils;
import org.apache.rocketmq.util.VerifyUtils;
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
public class PushConsumerRetryTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(PushConsumerRetryTest.class);
    private String tag;
    private final static int SEND_NUM = 10;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @Test
    @DisplayName("The normal message is sent, and after the PushConsumer partial retry, the retry message is expected to be consumed")
    public void testNormalTopicPushConsumerRetry() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Map<String, MessageView> noRetryMsgs = new ConcurrentHashMap<>();
        Map<String, MessageView> retryMsgs = new ConcurrentHashMap<>();

        SimpleConsumer simpleConsumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer);

        try {
            provider.newPushConsumerBuilder().setClientConfiguration(ClientConfigurationFactory.build(account)).setConsumerGroup(groupId).setSubscriptionExpressions(Collections.singletonMap(topic, new FilterExpression(tag, FilterExpressionType.TAG))).setConsumptionThreadCount(20).setMessageListener(messageView -> {
                if (Integer.parseInt(StandardCharsets.UTF_8.decode(messageView.getBody()).toString()) % 2 == 0) {
                    log.info(String.format("commit normal msg %s ", messageView));
                    noRetryMsgs.putIfAbsent(messageView.getMessageId().toString(), messageView);
                    return ConsumeResult.SUCCESS;
                } else if (messageView.getDeliveryAttempt() == 2) {
                    log.info(String.format("retry  normal msg %s ", messageView));
                    retryMsgs.putIfAbsent(messageView.getMessageId().toString(), messageView);
                    return ConsumeResult.SUCCESS;
                }
                return ConsumeResult.FAILURE;
            }).build();
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(producer, "Get Producer Failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        //All messages are consumed.
        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return noRetryMsgs.size() == retryMsgs.size() && noRetryMsgs.size() == SEND_NUM / 2;
            }
        });
        for (Object msgId : producer.getEnqueueMessages().getAllData()) {
            Assertions.assertTrue(noRetryMsgs.containsKey(msgId) || retryMsgs.containsKey(msgId));
        }
    }

    @Test
    @DisplayName("The send order message, after the PushConsumer partial retry, is expected to consume the retry message, and the message consumption order and send order")
    public void testFiFoTopicPushConsumerRetry() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        String groupId = getOrderlyGroupId(methodName);

        SimpleConsumer simpleConsumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer);

        PushConsumer pushConsumer = null;
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        Vector<MessageView> recvMessages = new Vector<>();
        try {
            pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, new FilterExpression(tag)))
                .setConsumptionThreadCount(20)
                .setMessageListener(messageView -> {
                    if (Integer.parseInt(StandardCharsets.UTF_8.decode(messageView.getBody()).toString()) % 2 == 0) {
                        log.info(String.format("commit fifo msg %s ", messageView));
                        recvMessages.add(messageView);
                        return ConsumeResult.SUCCESS;
                    } else if (messageView.getDeliveryAttempt() == 2) {
                        log.info(String.format("retry fifo msg %s ", messageView));
                        recvMessages.add(messageView);
                        return ConsumeResult.SUCCESS;
                    }
                    return ConsumeResult.FAILURE;
                }).build();
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(producer, "Get Producer Failed");
        String messageGroup = RandomUtils.getStringByUUID();
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), messageGroup);
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        //All messages are consumed.
        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return recvMessages.size() == SEND_NUM;
            }
        });
        for (int i = 0; i < SEND_NUM; i++) {
            Assertions.assertEquals(i, Integer.parseInt(StandardCharsets.UTF_8.decode(recvMessages.get(i).getBody()).toString()), "recv message failed");
        }
    }

    @Test
    @DisplayName("Send sequential messages, using three Shardingkeys, after partial retries, expect to consume retry messages, and the order of message consumption and the order of message delivery")
    public void testFiFoTopicWith3ShardingkeyPushConsumerRetry() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.FIFO.getValue(), methodName);
        String groupId = getOrderlyGroupId(methodName);

        SimpleConsumer simpleConsumer = ConsumerFactory.getSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer);

        PushConsumer pushConsumer = null;
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);
        Vector<MessageView> recvMessages = new Vector<>();
        try {
            pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, new FilterExpression(tag)))
                .setConsumptionThreadCount(20)
                .setMessageListener(messageView -> {
                    if (Integer.parseInt(StandardCharsets.UTF_8.decode(messageView.getBody()).toString()) % 2 == 0) {
                        log.info(String.format("commit fifo msg %s ", messageView));
                        recvMessages.add(messageView);
                        return ConsumeResult.SUCCESS;
                    } else if (messageView.getDeliveryAttempt() == 2) {
                        log.info(String.format("retry fifo msg %s ", messageView));
                        recvMessages.add(messageView);
                        return ConsumeResult.SUCCESS;
                    }
                    return ConsumeResult.FAILURE;
                }).build();
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(producer, "Get Producer Failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), String.valueOf(i % 3));
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        //All messages are consumed.
        await().atMost(180, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return recvMessages.size() == SEND_NUM;
            }
        });

        ArrayList<MessageView> shardingKeyA = new ArrayList<MessageView>();
        ArrayList<MessageView> shardingKeyB = new ArrayList<MessageView>();
        ArrayList<MessageView> shardingKeyC = new ArrayList<MessageView>();
        for (int i = 0; i < SEND_NUM; i++) {
            MessageView messageView = recvMessages.get(i);
            switch (messageView.getMessageGroup().toString()) {
                case "Optional[a]":
                    shardingKeyA.add(messageView);
                    break;
                case "Optional[b]":
                    shardingKeyB.add(messageView);
                    break;
                case "Optional[c]":
                    shardingKeyC.add(messageView);
                    break;
                default:
                    log.info(String.format("recv msgs with wrong shardingkey: %s, %s",
                        messageView.getMessageGroup(), messageView));
            }
        }
        Assertions.assertTrue(isSorted(shardingKeyA));
        Assertions.assertTrue(isSorted(shardingKeyB));
        Assertions.assertTrue(isSorted(shardingKeyC));
    }

    public static boolean isSorted(List<MessageView> messageViewList) {
        for (int i = 0; i < messageViewList.size() - 1; i++) {
            if (Integer.parseInt(StandardCharsets.UTF_8.decode(messageViewList.get(i).getBody()).toString())
                > Integer.parseInt(StandardCharsets.UTF_8.decode(messageViewList.get(i + 1).getBody()).toString())) {
                return false;
            }
        }
        return true;
    }

}

