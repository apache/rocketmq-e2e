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

package org.apache.rocketmq.server.abnormal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.apache.rocketmq.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(TESTSET.RETRY)
@Tag(TESTSET.SMOKE)
@Execution(ExecutionMode.SAME_THREAD)
public class PushConsumerExceptionTest extends BaseOperate{
    private final Logger log = LoggerFactory.getLogger(PushConsumerExceptionTest.class);
    private final static int SEND_NUM = 5;

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Simulate pushconsumer consumption fail, expect that the original message was not received, and capture all messages after message retry")
    public void testExceptionConsumption() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Map<String, MessageExt> firstMsgs = new ConcurrentHashMap<>();
        Map<String, MessageExt> retryMsgs = new ConcurrentHashMap<>();

        DefaultMQPushConsumer pushConsumer = null;
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.setMaxReconsumeTimes(2);
            
            pushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() == 2) {
                            retryMsgs.putIfAbsent(msg.getMsgId(), msg);
                            log.info("consume success: {}", msg);
                        } else {
                            // Simulate consuming operations
                            log.info("{}", "Simulate consuming operations fail");
                            int i = 1 / 0;
                            log.info("{}", "Simulate consuming operations fail end");
                            firstMsgs.putIfAbsent(msg.getMsgId(), msg);
                            log.info(String.format("recv msg(fail) %s ", msg));
                        }

                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

            });
            pushConsumer.start();
        } catch (MQClientException e) {
            Assertions.fail(e.getMessage());
        }
        Assertions.assertNotNull(producer, "Get Producer Failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return retryMsgs.size() == SEND_NUM && firstMsgs.size() == 0;
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Simulate pushconsumer consumption return null, expect that the original message was not received, and capture all messages after message retry")
    public void testNullConsumption() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Map<String, MessageExt> firstMsgs = new ConcurrentHashMap<>();
        Map<String, MessageExt> retryMsgs = new ConcurrentHashMap<>();

        DefaultMQPushConsumer pushConsumer = null;
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.setMaxReconsumeTimes(2);
            
            pushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() == 2) {
                            retryMsgs.putIfAbsent(msg.getMsgId(), msg);
                            log.info("consume success: {}", msg);
                        } else {
                            // Simulate consuming operations
                            log.info("{}", "Simulate consuming operations return null");
                            firstMsgs.putIfAbsent(msg.getMsgId(), msg);
                            log.info(String.format("recv msg(null) %s ", msg));
                            return null;
                        }

                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            pushConsumer.start();
        } catch (MQClientException e) {
            Assertions.fail(e.getMessage());
        }
        Assertions.assertNotNull(producer, "Get Producer Failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(180, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return retryMsgs.size() == SEND_NUM && firstMsgs.size() == SEND_NUM;
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Disabled
    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Simulate pushconsumer consumption timeout, expect message retry")
    public void testTimeoutConsumption() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Map<String, MessageExt> firstMsgs = new ConcurrentHashMap<>();
        Map<String, MessageExt> retryMsgs = new ConcurrentHashMap<>();

        DefaultMQPushConsumer pushConsumer = null;
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.setMaxReconsumeTimes(3);
            
            pushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    try {
                        for (MessageExt msg : msgs) {
                            if (msg.getReconsumeTimes() == 0) {
                                // Simulate time-consuming operations
                                log.info("{}", "Simulate time-consuming operations");
                                Thread.sleep(65000); // Assume that message processing takes 20 seconds
                                log.info("{}", "Simulate time-consuming operations end");
                                firstMsgs.putIfAbsent(msg.getMsgId(), msg);
                                log.info(String.format("recv msg(timeout) %s ", msg));
                            } else {
                                retryMsgs.putIfAbsent(msg.getMsgId(), msg);
                                log.info("consume success: {}", msg);
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            pushConsumer.start();
        } catch (MQClientException e) {
            Assertions.fail(e.getMessage());
        }
        Assertions.assertNotNull(producer, "Get Producer Failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(60, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return retryMsgs.size() == SEND_NUM;
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }
}
