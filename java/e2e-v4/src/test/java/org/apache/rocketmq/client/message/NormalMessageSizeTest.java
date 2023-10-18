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

package org.apache.rocketmq.client.message;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.TransactionListenerImpl;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.NORMAL)
@DisplayName("Test cases that send messages")
public class NormalMessageSizeTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(NormalMessageSizeTest.class);
    private static String normalTopic;
    private static String transTopic;
    private static String fifoTopic;
    private static String delayTopic;
    private static DefaultMQProducer producer;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        normalTopic = getTopic(methodName);
        transTopic = getTopic(methodName);
        delayTopic = getTopic(methodName);
        fifoTopic = getTopic(methodName);
    }

    @AfterAll
    public static void tearDownAll() {
        if (producer != null) {
            producer.shutdown();
        }
    }

    @Test
    @DisplayName("Send normal messages synchronously with the body size of 4M+1, expect send failed")
    public void testNormalMsgSize4MAdd1() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }

        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = new Message(normalTopic, tag, messageBody.getBytes());
            producer.send(message);
        });
    }

    @Test
    @DisplayName("Send normal messages synchronously with the body size of 4M, expect send success")
    public void testNormalMsgSize4M() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = new Message(normalTopic, tag, messageBody.getBytes());
        try {
            producer.send(message);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Send message failed, expected success");
        }
    }

    @Test
    @DisplayName("Send delay messages synchronously with the body size of 4M+1, expect send failed")
    public void testDelayMsgSize4MAdd1() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = new Message(delayTopic, tag, messageBody.getBytes());
            message.setDelayTimeLevel(3);
            producer.send(message);
        });
    }

    @Test
    @DisplayName("Send delay messages synchronously with the body size of 4M, expect send success")
    public void testDelayMsgSize4M() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = new Message(delayTopic, tag, messageBody.getBytes());
        message.setDelayTimeLevel(3);
        try {
            producer.send(message);
        } catch (MQClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (MQBrokerException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (RemotingException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (InterruptedException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send transaction messages synchronously with the body size of 4M+1, expect send failed")
    public void testTransMsgSize4MAdd1() {
        TransactionMQProducer producer = new TransactionMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        TransactionListenerImpl transactionListener = new TransactionListenerImpl(LocalTransactionState.COMMIT_MESSAGE,
                LocalTransactionState.COMMIT_MESSAGE);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });
        try {
            if (executorService != null) {
                producer.setExecutorService(executorService);
            }
            producer.setTransactionListener(transactionListener);
            producer.start();
        } catch (MQClientException e) {
            log.info("Start TransactionMQProducer failed, {}", e.getMessage());
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        assertThrows(Exception.class, () -> {
            Message message = new Message(transTopic, tag, messageBody.getBytes());
            producer.sendMessageInTransaction(message, null);
        });
        producer.shutdown();
    }

    @Test
    @DisplayName("Send transaction messages synchronously with the body size of 4M, expect send success")
    public void testTransMsgSize4M() {
        TransactionMQProducer producer = new TransactionMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        TransactionListenerImpl transactionListener = new TransactionListenerImpl(LocalTransactionState.COMMIT_MESSAGE,
                LocalTransactionState.COMMIT_MESSAGE);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });
        try {
            if (executorService != null) {
                producer.setExecutorService(executorService);
            }
            producer.setTransactionListener(transactionListener);
            producer.start();
        } catch (MQClientException e) {
            log.info("Start TransactionMQProducer failed, {}", e.getMessage());
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = new Message(transTopic, tag, messageBody.getBytes());
        try {
            producer.sendMessageInTransaction(message, null);
        } catch (MQClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
        producer.shutdown();
    }

    @Test
    @DisplayName("Send FIFO messages synchronously with the body size of 4M+1, expect send failed")
    public void testFifoMsgSize4MAdd1() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        List<MessageQueue> messageQueues = null;
        try {
            messageQueues = producer.fetchPublishMessageQueues(fifoTopic);
        } catch (MQClientException e) {
            Assertions.assertNotNull(messageQueues);
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024 + 1);
        String tag = NameUtils.getRandomTagName();
        List<MessageQueue> finalMessageQueues = messageQueues;
        assertThrows(Exception.class, () -> {
            Message message = new Message(fifoTopic, tag, messageBody.getBytes());
            if (finalMessageQueues.size() > 0) {
                producer.send(message, finalMessageQueues.get(0));
            }
            producer.send(message);
        });
    }

    @Test
    @DisplayName("Send FIFO messages synchronously with the body size of 4M, expect send success")
    public void testFifoMsgSize4M() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        List<MessageQueue> messageQueues = null;
        try {
            messageQueues = producer.fetchPublishMessageQueues(fifoTopic);
        } catch (MQClientException e) {
            Assertions.assertNotNull(messageQueues);
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String tag = NameUtils.getRandomTagName();
        Message message = new Message(fifoTopic, tag, messageBody.getBytes());
        List<MessageQueue> finalMessageQueues = messageQueues;
        try {
            if (finalMessageQueues.size() > 0) {
                producer.send(message, finalMessageQueues.get(0));
            }
        } catch (MQBrokerException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (RemotingException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (InterruptedException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (MQClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send normal messages synchronously with the body size of 4M and the user property size of 16KB, expect send success")
    public void testNormalMsgSize4MAndUserProperty16KB() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String key = RandomStringUtils.randomAlphabetic(8 * 1024);
        String value = RandomStringUtils.randomAlphabetic(8 * 1024);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);
        try {
            Message message = new Message(normalTopic, messageBody.getBytes());
            for (Map.Entry<String, String> entry : userProperty.entrySet()) {
                message.putUserProperty(entry.getKey(), entry.getValue());
            }
            producer.send(message);
        } catch (MQBrokerException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (RemotingException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (InterruptedException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (MQClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("Send FIFO messages synchronously with the body size of 4M and the user property size of 16KB, expect send success")
    public void testFifoMsgSize4MAndUserProperty16KB() {
        producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        List<MessageQueue> messageQueues = null;
        try {
            messageQueues = producer.fetchPublishMessageQueues(fifoTopic);
        } catch (MQClientException e) {
            Assertions.assertNotNull(messageQueues);
        }
        String messageBody = RandomStringUtils.randomAlphabetic(4 * 1024 * 1024);
        String key = RandomStringUtils.randomAlphabetic(8 * 1024);
        String value = RandomStringUtils.randomAlphabetic(8 * 1024);
        HashMap<String, String> userProperty = new HashMap<>();
        userProperty.put(key, value);
        List<MessageQueue> finalMessageQueues = messageQueues;
        try {
            Message message = new Message(fifoTopic, messageBody.getBytes());
            for (Map.Entry<String, String> entry : userProperty.entrySet()) {
                message.putUserProperty(entry.getKey(), entry.getValue());
            }
            if (finalMessageQueues.size() > 0) {
                producer.send(message, finalMessageQueues.get(0));
            }
        } catch (MQBrokerException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (RemotingException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (InterruptedException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        } catch (MQClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

}
