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
public class PushConsumerTimeTest extends BaseOperate{
    private final Logger log = LoggerFactory.getLogger(PushConsumerTimeTest.class);
    private final static int SEND_NUM = 5;

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Send normal messages, set the maximum number of retries and set the received messages to RECONSUME_LATER. The expected retry time is about 10s for the first time and about 30s for the second time")
    public void testNormalMessageReconsumeTime() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Map<String, Instant> msgsReConsumeTime = new ConcurrentHashMap<>();
        Map<String, Boolean> msgsReachedTwoReconsumeTimes = new ConcurrentHashMap<>();
        Lock lock = new ReentrantLock();

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
                        if (msg.getReconsumeTimes() == 0) {
                            msgsReConsumeTime.putIfAbsent(msg.getMsgId(), Instant.now());
                            msgsReachedTwoReconsumeTimes.putIfAbsent(msg.getMsgId(), false);
                            log.info(String.format("recv msg(first) %s ", msg));
                        } else {
                            Instant nowTime = Instant.now();
                            if (msgsReConsumeTime.containsKey(msg.getMsgId())) {
                                lock.lock();
                                try {
                                    Instant lastTime = msgsReConsumeTime.get(msg.getMsgId());
                                    Duration duration = Duration.between(lastTime, nowTime);
                                    if (msg.getReconsumeTimes() == 1) {
                                        log.info("first retry time is: {}", duration.getSeconds());
                                        Assertions.assertTrue(duration.getSeconds() < 20);
                                    } else {
                                        log.info("second retry time is: {}", duration.getSeconds());
                                        Assertions.assertTrue(duration.getSeconds() < 60);
                                        msgsReachedTwoReconsumeTimes.put(msg.getMsgId(), true);
                                    }
                                    msgsReConsumeTime.put(msg.getMsgId(), nowTime);
                                } finally {
                                    lock.unlock();
                                }
                            } else {
                                msgsReConsumeTime.putIfAbsent(msg.getMsgId(), Instant.now());
                            }

                            log.info(String.format("recv msgid(reconsume later) %s ", msg.getMsgId()));
                        }
                    }
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() != 2) {
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
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
                boolean flag = true;
                for (Map.Entry<String, Boolean> entry : msgsReachedTwoReconsumeTimes.entrySet()) {
                    if (!entry.getValue()) {
                        flag = false;
                        break;
                    }
                }
                return msgsReachedTwoReconsumeTimes.size() == SEND_NUM && flag;
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Send order messages, set the maximum number of retries and set the received messages to SUSPEND_CURRENT_QUEUE_A_MOMENT. The expected retry time is about 1s")
    public void testOrderMessageReconsumeTime() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Map<String, Instant> msgsReConsumeTime = new ConcurrentHashMap<>();
        Map<String, Boolean> msgsReachedTwoReconsumeTimes = new ConcurrentHashMap<>();
        Lock lock = new ReentrantLock();

        DefaultMQPushConsumer pushConsumer = null;
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.setMaxReconsumeTimes(2);
            
            pushConsumer.setMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeOrderlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() == 0) {
                            msgsReConsumeTime.putIfAbsent(msg.getMsgId(), Instant.now());
                            msgsReachedTwoReconsumeTimes.putIfAbsent(msg.getMsgId(), false);
                            log.info(String.format("recv msg(first) %s ", msg));
                        } else {
                            Instant nowTime = Instant.now();
                            if (msgsReConsumeTime.containsKey(msg.getMsgId())) {
                                lock.lock();
                                try {
                                    Instant lastTime = msgsReConsumeTime.get(msg.getMsgId());
                                    Duration duration = Duration.between(lastTime, nowTime);
                                    if (msg.getReconsumeTimes() == 1) {
                                        log.info("first retry time is: {}", duration.getSeconds());
                                        Assertions.assertTrue(duration.getSeconds() < 2);
                                    } else {
                                        log.info("second retry time is: {}", duration.getSeconds());
                                        Assertions.assertTrue(duration.getSeconds() < 2);
                                        msgsReachedTwoReconsumeTimes.put(msg.getMsgId(), true);
                                    }
                                    msgsReConsumeTime.put(msg.getMsgId(), nowTime);
                                } finally {
                                    lock.unlock();
                                }
                            } else {
                                msgsReConsumeTime.putIfAbsent(msg.getMsgId(), Instant.now());
                            }

                            log.info(String.format("recv msgid(reconsume later) %s ", msg.getMsgId()));
                        }
                    }
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() != 2) {
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }

            });
            pushConsumer.start();
        } catch (MQClientException e) {
            Assertions.fail(e.getMessage());
        }

        Assertions.assertNotNull(producer, "Get Producer Failed");
        List<MessageQueue> mqs = producer.fetchPublishMessageQueues(topic);
        List<MessageQueue> sendMqs = new ArrayList<>();
        sendMqs.add(mqs.get(0));
        producer.sendWithQueue(sendMqs, SEND_NUM, tag);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                boolean flag = true;
                for (Map.Entry<String, Boolean> entry : msgsReachedTwoReconsumeTimes.entrySet()) {
                    if (!entry.getValue()) {
                        flag = false;
                        break;
                    }
                }
                return msgsReachedTwoReconsumeTimes.size() == SEND_NUM && flag;
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Send normal messages, set the maximum consumption to 0(The first retry is 10 seconds, and the setting is 15 seconds. Then check whether the retry occurs), and set the message reception to RECONUME_LATER, expecting that the retry will not occur")
    public void testNormalMessageRetryTimesSetting() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Set<String> msgsRecv = new ConcurrentSkipListSet<>();

        DefaultMQPushConsumer pushConsumer = null;
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.setMaxReconsumeTimes(0);
            
            pushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() > 0) {
                            Assertions.fail("retry times is not equal to maxReconsumeTimes");
                        } else {
                            log.info("retry times is equal to maxReconsumeTimes: {}", msg.getMsgId());
                            msgsRecv.add(msg.getMsgId());
                            log.info("recv msgid(reconsume later) {} ", msg.getMsgId());
                        }
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
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
                return SEND_NUM == msgsRecv.size();
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @DisplayName("Send one order message, set the maximum consumption to 0(The retry time of each message is 1s. Then check whether the retry occurs), and set the message reception to SUSPEND_CURRENT_QUEUE_A_MOMENT, expecting that the retry will not occur")
    public void testOrderMessageRetryTimesSetting() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String tag = NameUtils.getRandomTagName();
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Map<String, Integer> msgsReConsumeTimes = new ConcurrentHashMap<>();

        DefaultMQPushConsumer pushConsumer = null;
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.setMaxReconsumeTimes(30);
            
            pushConsumer.setMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeOrderlyContext context) {
                    for (MessageExt msg : msgs) {
                        if (msg.getReconsumeTimes() > 30) {
                            Assertions.fail("retry times is not equal to maxReconsumeTimes");
                        } else {
                            log.info("recv msgid(reconsume later) {}, reconsumer times is {}", msg.getMsgId(), msg.getReconsumeTimes());
                            if(msgsReConsumeTimes.containsKey(msg.getMsgId())) {
                                if(msgsReConsumeTimes.get(msg.getMsgId()) < msg.getReconsumeTimes()) {
                                    msgsReConsumeTimes.put(msg.getMsgId(), msg.getReconsumeTimes());
                                }
                            }else{
                                msgsReConsumeTimes.putIfAbsent(msg.getMsgId(), msg.getReconsumeTimes());
                            }
                        }
                    }
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }

            });
            pushConsumer.start();
        } catch (MQClientException e) {
            Assertions.fail(e.getMessage());
        }

        Assertions.assertNotNull(producer, "Get Producer Failed");
        List<MessageQueue> mqs = producer.fetchPublishMessageQueues(topic);
        List<MessageQueue> sendMqs = new ArrayList<>();
        sendMqs.add(mqs.get(0));
        producer.sendWithQueue(sendMqs, 1, tag);
        Assertions.assertEquals(1, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(60, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return msgsReConsumeTimes.size() == 1 && msgsReConsumeTimes.values().iterator().next() == 30;
            }
        });

        producer.shutdown();
        pushConsumer.shutdown();
    }
}
