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

package org.apache.rocketmq.cluster;

import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQIdempotentListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQOrderListener;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.VerifyUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(TESTSET.LOAD_BALANCING)
public class LoadBalancingTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(LoadBalancingTest.class);
    private String tag;
    private final static int SEND_NUM = 10;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @Test
    @DisplayName("Normal message load balancing, start 4 consumers, send 240 messages, expect 4 consumers to consume load balancing, each consume 1/4, then shutdown 2 of them, send 240 messages again, still load balancing, each consume half, and start 2 new consumers. Another 240 messages are sent, still load balanced, each consuming 1/4")
    public void testLoadBalancing_normal_message() {
        int messageSize = 240;
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        RMQNormalConsumer consumer1 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer2 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer3 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer4 = ConsumerFactory.getRMQClusterConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        consumer1.subscribeAndStart(topic, tag, new RMQIdempotentListener());
        consumer2.subscribeAndStart(topic, tag, new RMQIdempotentListener());
        consumer3.subscribeAndStart(topic, tag, new RMQIdempotentListener());
        consumer4.subscribeAndStart(topic, tag, new RMQIdempotentListener());

        Assertions.assertNotNull(producer);
        producer.send(topic, tag, messageSize);

        Assertions.assertEquals(messageSize, producer.getEnqueueMessages().getDataSize(), "send message failed");

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer1.getListener().getDequeueMessages().getDataSize() +
                       consumer2.getListener().getDequeueMessages().getDataSize() +
                       consumer3.getListener().getDequeueMessages().getDataSize() +
                       consumer4.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });

        VerifyUtils.verifyBalance(messageSize, consumer1.getListener().getDequeueMessages().getDataSize(),
                                                consumer2.getListener().getDequeueMessages().getDataSize(),
                                                consumer3.getListener().getDequeueMessages().getDataSize(),
                                                consumer4.getListener().getDequeueMessages().getDataSize());

        consumer1.getListener().clearMsg();
        consumer2.getListener().clearMsg();

        consumer3.shutdown();
        consumer4.shutdown();

        producer.send(topic, tag, messageSize);

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer1.getListener().getDequeueMessages().getDataSize() +
                       consumer2.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });

        VerifyUtils.verifyBalance(messageSize, consumer1.getListener().getDequeueMessages().getDataSize(),
                                                consumer2.getListener().getDequeueMessages().getDataSize());

        consumer1.getListener().clearMsg();
        consumer2.getListener().clearMsg();

        RMQNormalConsumer consumer5 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer6 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        consumer5.subscribeAndStart(topic, tag, new RMQIdempotentListener());
        consumer6.subscribeAndStart(topic, tag, new RMQIdempotentListener());

        producer.send(topic, tag, messageSize);

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer1.getListener().getDequeueMessages().getDataSize() +
                       consumer2.getListener().getDequeueMessages().getDataSize() +
                       consumer5.getListener().getDequeueMessages().getDataSize() +
                       consumer6.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });

        VerifyUtils.verifyBalance(messageSize, consumer1.getListener().getDequeueMessages().getDataSize(),
                                                consumer2.getListener().getDequeueMessages().getDataSize(),
                                                consumer5.getListener().getDequeueMessages().getDataSize(),
                                                consumer6.getListener().getDequeueMessages().getDataSize());

        producer.shutdown();
        consumer1.shutdown();
        consumer2.shutdown();
        consumer5.shutdown();
        consumer6.shutdown();
    }

    @Test
    @DisplayName("Global sequential message load balancing: Start 2 consumers, send 30 messages, expect only 1 Consumer to consume the message, and the other Consumer to consume 0, then shutdown 1 Consumer with message consumption, send another 30 messages, expect the idling Consumer to pull the message")
    public void testLoadBalancing_global_sequential_message(){
        int messageSize = 30;
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        RMQNormalConsumer pullConsumer = ConsumerFactory.getRMQLitePullConsumer(namesrvAddr, groupId, rpcHook,1);
        pullConsumer.subscribeAndStartLitePull(topic,MessageSelector.byTag(tag));
        VerifyUtils.tryReceiveOnce(pullConsumer.getLitePullConsumer());
        pullConsumer.shutdown();

        RMQNormalConsumer consumer1 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer2 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        consumer1.subscribeAndStart(topic, tag, new RMQOrderListener());
        consumer2.subscribeAndStart(topic, tag, new RMQOrderListener());

        Assertions.assertNotNull(producer);
        List<MessageQueue> msgQueues = producer.fetchPublishMessageQueues(topic);
        List<MessageQueue> msgQueue = new ArrayList<>();
        msgQueue.add(msgQueues.get(0));
        producer.sendWithQueue(msgQueue,tag,messageSize);

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer1.getListener().getDequeueMessages().getDataSize() +
                       consumer2.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });
        Assertions.assertTrue(consumer1.getListener().getDequeueMessages().getDataSize() == 0 || consumer2.getListener().getDequeueMessages().getDataSize() == 0, "global sequential message load balancing is not satisfied");

        consumer1.shutdown();

        consumer2.getListener().clearMsg();

        producer.sendWithQueue(msgQueue,tag,messageSize);

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer2.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });

        Assertions.assertTrue(consumer2.getListener().getDequeueMessages().getDataSize() == messageSize, "global sequential message load balancing is not satisfied");

        producer.shutdown();
        consumer2.shutdown();
    }

    @Test
    @DisplayName("Partition sequential message load balancing, start 4 consumers, send 120 messages, shardingkey=8, expect 4 consumers to consume load balancing, each consumes 1/4 Partition, shutdown 2 of the consumers, send 120 messages again. The load is still balanced, and half of each Partition is consumed")
    public void testLoadBalancing_partition_sequential_message(){
        int messageSize = 120;
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);

        RMQNormalConsumer pullConsumer = ConsumerFactory.getRMQLitePullConsumer(namesrvAddr, groupId, rpcHook,1);
        pullConsumer.subscribeAndStartLitePull(topic,MessageSelector.byTag(tag));
        VerifyUtils.tryReceiveOnce(pullConsumer.getLitePullConsumer());
        pullConsumer.shutdown();

        RMQNormalConsumer consumer1 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer2 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer3 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        RMQNormalConsumer consumer4 = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook,new AllocateMessageQueueAveragely());
        consumer1.subscribeAndStart(topic, tag, new RMQOrderListener());
        consumer2.subscribeAndStart(topic, tag, new RMQOrderListener());
        consumer3.subscribeAndStart(topic, tag, new RMQOrderListener());
        consumer4.subscribeAndStart(topic, tag, new RMQOrderListener());

        Assertions.assertNotNull(producer);
        for (int i = 0; i < messageSize; i++) {
            Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag, String.valueOf(i));
            try {
                SendResult sendResult = producer.getProducer().send(message, new MessageQueueSelector(){

                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer index = (Integer) arg;
                        return mqs.get(index % mqs.size());
                    }
                    
                },i);
                log.info("{}, index: {}, tag: {}", sendResult, i, tag);
            } catch (Exception e) {
                Assertions.fail("DefaultMQProducer send message failed");
            }
        }

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer1.getListener().getDequeueMessages().getDataSize() +
                       consumer2.getListener().getDequeueMessages().getDataSize() +
                       consumer3.getListener().getDequeueMessages().getDataSize() +
                       consumer4.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });

        Assertions.assertTrue(messageSize/4==consumer1.getListener().getDequeueMessages().getDataSize(), "consumer1: first load balancing is not satisfied");
        Assertions.assertTrue(messageSize/4==consumer2.getListener().getDequeueMessages().getDataSize(), "consumer2: first load balancing is not satisfied");
        Assertions.assertTrue(messageSize/4==consumer3.getListener().getDequeueMessages().getDataSize(), "consumer3: first load balancing is not satisfied");
        Assertions.assertTrue(messageSize/4==consumer4.getListener().getDequeueMessages().getDataSize(), "consumer4: first load balancing is not satisfied");

        consumer3.shutdown();
        consumer4.shutdown();

        consumer1.getListener().clearMsg();
        consumer2.getListener().clearMsg();

        for (int i = 0; i < messageSize; i++) {
            Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag, String.valueOf(i));
            try {
                SendResult sendResult = producer.getProducer().send(message, new MessageQueueSelector(){

                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer index = (Integer) arg;
                        return mqs.get(index % mqs.size());
                    }
                    
                },i);
                log.info("{}, index: {}, tag: {}", sendResult, i, tag);
            } catch (Exception e) {
                Assertions.fail("DefaultMQProducer send message failed");
            }
        }

        await().atMost(120, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return consumer1.getListener().getDequeueMessages().getDataSize() +
                       consumer2.getListener().getDequeueMessages().getDataSize() == messageSize;
            }
        });

        Assertions.assertTrue(messageSize/2==consumer1.getListener().getDequeueMessages().getDataSize(), "consumer1: second load balancing is not satisfied");
        Assertions.assertTrue(messageSize/2==consumer2.getListener().getDequeueMessages().getDataSize(), "consumer2: second load balancing is not satisfied");

        producer.shutdown();
        consumer1.shutdown();
        consumer2.shutdown();
    }
}
