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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.server.transaction;

import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQTransactionProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.listener.rmq.concurrent.TransactionListenerImpl;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Tag(TESTSET.TRANSACTION)
@Tag(TESTSET.SMOKE)
public class TransactionMessageTest extends BaseOperate {
    private final Logger logger = LoggerFactory.getLogger(TransactionMessageTest.class);
    private String tag;
    private String topic;
    private String groupId;
    private final static int SEND_NUM = 10;

    @BeforeEach
    public void setUp() {
        topic = NameUtils.getTopicName();
        tag = NameUtils.getTagName();
        groupId = NameUtils.getGroupName();
        MQAdmin.createTopic(namesrvAddr, cluster, topic, 8);
        logger.info("topic:{}, tag:{}, groupId:{}", topic, tag, groupId);
    }

    @Test
    @DisplayName("Send 10 transaction messages synchronously, expecting all to be consumed")
    public void testConsumeNormalMessage() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });

        RMQTransactionProducer producer = ProducerFactory.getTransProducer(namesrvAddr, executorService,
                new TransactionListenerImpl(LocalTransactionState.COMMIT_MESSAGE, LocalTransactionState.COMMIT_MESSAGE),
                rpcHook);
        producer.sendTrans(topic, tag, SEND_NUM);

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());
        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    @DisplayName("Send 10 transaction messages and rollback directly (Checker does commit), expecting that these 10 messages cannot be consumed by PushConsumer")
    public void testTrans_SendRollback_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.byTag(tag), new RMQNormalListener());

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });

        RMQTransactionProducer producer = ProducerFactory.getTransProducer(namesrvAddr, executorService,
                new TransactionListenerImpl(LocalTransactionState.COMMIT_MESSAGE,
                        LocalTransactionState.ROLLBACK_MESSAGE),
                rpcHook);
        producer.sendTrans(topic, tag, SEND_NUM);
        // Wait for the callback, expecting not to commit the already rolled back
        // message
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @DisplayName("Send 10 transaction messages and COMMIT the transaction by Checker (perform COMMIT), expecting the 10 messages to be consumed by PushConsumer")
    public void testTrans_SendCheckerCommit_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.byTag(tag), new RMQNormalListener());

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });

        RMQTransactionProducer producer = ProducerFactory.getTransProducer(namesrvAddr, executorService,
                new TransactionListenerImpl(LocalTransactionState.COMMIT_MESSAGE, LocalTransactionState.UNKNOW),
                rpcHook);
        producer.sendTrans(topic, tag, SEND_NUM);

        // Wait for the callback to execute commit
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @DisplayName("Send 10 transaction messages and roll back the transaction by Checker (performing ROLLBACK), expecting that the 10 messages will not be consumed by PushConsumer")
    public void testTrans_CheckerRollback() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.byTag(tag), new RMQNormalListener());

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });

        RMQTransactionProducer producer = ProducerFactory.getTransProducer(namesrvAddr, executorService,
                new TransactionListenerImpl(LocalTransactionState.ROLLBACK_MESSAGE, LocalTransactionState.UNKNOW),
                rpcHook);
        producer.sendTrans(topic, tag, SEND_NUM);
        // Wait for the rollback and execute rollback
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
        producer.shutdown();
        pushConsumer.shutdown();
    }

    @Test
    @DisplayName("Send 10 transactional messages and commit them by checking back (Checker commits for partial messages), and the expected committed messages can be consumed by PushConsumer")
    public void testTrans_SendCheckerPartionCommit() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.byTag(tag), new RMQNormalListener());

        AtomicInteger commitMsgNum = new AtomicInteger(0);
        AtomicInteger rollbackMsgNum = new AtomicInteger(0);

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });

        RMQTransactionProducer producer = ProducerFactory.getTransProducer(namesrvAddr, executorService,
                new TransactionListener() {

                    @Override
                    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                        return LocalTransactionState.UNKNOW;
                    }

                    @Override
                    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                        if (Integer
                                .parseInt(String.valueOf(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(msg.getBody()))))
                                % 2 == 0) {
                            commitMsgNum.getAndIncrement();
                            return LocalTransactionState.COMMIT_MESSAGE;
                        } else {
                            rollbackMsgNum.getAndIncrement();
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                        }
                    }

                }, rpcHook);
        producer.sendTrans(topic, tag, SEND_NUM);
        Assertions.assertNotNull(producer);

        await().atMost(90, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return rollbackMsgNum.get() == commitMsgNum.get() && commitMsgNum.get() == SEND_NUM / 2;
            }
        });
        // Wait for the rollback and execute commit/rollback
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Assertions.assertEquals(5, pushConsumer.getListener().getDequeueMessages().getDataSize());
        producer.shutdown();
        pushConsumer.shutdown();
    }

}
