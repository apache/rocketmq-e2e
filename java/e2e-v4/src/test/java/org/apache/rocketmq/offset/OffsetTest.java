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

package org.apache.rocketmq.offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.server.batch.BatchProducerTest;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Tag(TESTSET.OFFSET)
public class OffsetTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(OffsetTest.class);
    private String tag;
    private final static int SEND_NUM = 10;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @Test
    @DisplayName("Send 10 messages, set other groupid's pushconsumer consumption from first, expect to accept all messages again")
    public void testConsumeFromFisrtOffset() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId1 = getGroupId(methodName);
        String groupId2 = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        RMQNormalConsumer pullConsumer = ConsumerFactory.getRMQLitePullConsumer(namesrvAddr, groupId1, rpcHook, 1);
        pullConsumer.subscribeAndStartLitePull(topic, MessageSelector.byTag(tag));
        VerifyUtils.tryReceiveOnce(pullConsumer.getLitePullConsumer());
        pullConsumer.shutdown();

        DefaultMQPushConsumer pushConsumer;
        ConcurrentLinkedDeque<MessageExt> deque = new ConcurrentLinkedDeque<>();
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId1, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt message : msgs) {
                        log.info("receive message:{}", message);
                        deque.add(message);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            pushConsumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        Assertions.assertNotNull(producer);

        producer.send(topic, tag, SEND_NUM);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(30, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return deque.size() == SEND_NUM;
            }
        });

        pushConsumer.shutdown();
        deque.clear();

        DefaultMQPushConsumer reConsumer;
        try {
            reConsumer = new DefaultMQPushConsumer(groupId2, rpcHook, new AllocateMessageQueueAveragely());
            reConsumer.setInstanceName(RandomUtils.getStringByUUID());
            reConsumer.setNamesrvAddr(namesrvAddr);
            reConsumer.subscribe(topic, tag);
            reConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            reConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt message : msgs) {
                        log.info("reconsumer received message:{}", message);
                        deque.add(message);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            reConsumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        TestUtils.waitForSeconds(30);

        Assertions.assertEquals(SEND_NUM, deque.size(), "reconsumer receive message failed");

        reConsumer.shutdown();
        producer.shutdown();

    }

    @Test
    @DisplayName("Backlog 100 messages, start the consumer, and set the pull message from the LAST, expect to consume 100 messages")
    public void testConsumeFromLastOffset() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        Assertions.assertNotNull(producer);

        producer.send(topic, tag, 100);

        Assertions.assertEquals(100, producer.getEnqueueMessages().getDataSize(), "send message failed");

        DefaultMQPushConsumer pushConsumer;
        ConcurrentLinkedDeque<MessageExt> deque = new ConcurrentLinkedDeque<>();
        try {
            pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook, new AllocateMessageQueueAveragely());
            pushConsumer.setInstanceName(RandomUtils.getStringByUUID());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt message : msgs) {
                        log.info("receive message:{}", message);
                        deque.add(message);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            pushConsumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        TestUtils.waitForSeconds(30);
        Assertions.assertEquals(100, deque.size(), "consumer receive message failed");

        pushConsumer.shutdown();
        producer.shutdown();

    }

    @Test
    @DisplayName("send 10 messages, PullConsumer normally receives messages, but does not update messages offset, expect the messages are receive again")
    public void test_pull_receive_nack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQPullConsumer(namesrvAddr, groupId, rpcHook);
        consumer.startDefaultPull();
        VerifyUtils.tryReceiveOnce(consumer.getPullConsumer(), topic, tag, 32);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        producer.send(topic, tag, SEND_NUM);

        TestUtils.waitForSeconds(1);

        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");

        Set<MessageQueue> receiveMessageQueues = null;
        try {
            receiveMessageQueues = consumer.getPullConsumer().fetchSubscribeMessageQueues(topic);
        } catch (MQClientException e) {
            Assertions.fail("Fail to fetchSubscribeMessageQueues");
        }

        Collection<MessageExt> sendCollection = Collections
                .synchronizedCollection(producer.getEnqueueMessages().getAllData());
        Set<MessageQueue> finalMessageQueues = receiveMessageQueues;
        CompletableFuture[] futures = new CompletableFuture[receiveMessageQueues.size()];
        ConcurrentHashMap<String, AtomicInteger> messageMap = new ConcurrentHashMap<>();
        int mqCount = 0;
        for (MessageQueue mq : finalMessageQueues) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = consumer.getPullConsumer().fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        return null;
                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() < startTime + 5000) {
                        PullResult pullResult = consumer.getPullConsumer().pull(mq, tag, offset, SEND_NUM);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> messages = pullResult.getMsgFoundList();
                                for (MessageExt message : messages) {
                                    log.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())),
                                            message.getProperties(), message.getReconsumeTimes());
                                    sendCollection
                                            .removeIf(messageExt -> messageExt.getMsgId().equals(message.getMsgId()));
                                    if (messageMap.containsKey(message.getMsgId())) {
                                        messageMap.get(message.getMsgId()).incrementAndGet();
                                    } else {
                                        messageMap.put(message.getMsgId(), new AtomicInteger(1));
                                    }
                                }
                                break;
                            case NO_MATCHED_MSG:
                                break;
                            case NO_NEW_MSG:
                                break;
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                    }
                } catch (MQBrokerException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (RemotingException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (MQClientException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                }
                return null;
            });
            futures[mqCount++] = future;
        }
        try {
            CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("receive response count not match");
        }
        log.info("A total of {} messages were received", messageMap.size());
        for (Entry<String, AtomicInteger> entry : messageMap.entrySet()) {
            Assertions.assertTrue(entry.getValue().get() >= 1, "message receive count not match");
        }
    }
}
