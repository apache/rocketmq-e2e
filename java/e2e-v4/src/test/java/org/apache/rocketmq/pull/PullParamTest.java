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

package org.apache.rocketmq.pull;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.apache.rocketmq.utils.data.collect.DataCollector;
import org.apache.rocketmq.utils.data.collect.DataCollectorManager;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Tag(TESTSET.PULL)
public class PullParamTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(PullParamTest.class);
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

    @Test
    @DisplayName("Send 300 normal messages synchronously, and after using PullConsumer receive 30 messages, ack them after consuming them, expecting each receive to be less than or equal to 32 messages, and never receive the ack messages again")
    public void testNormal_pull_receive_maxsize_sync() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        int sendNum = 300;
        RMQNormalConsumer consumer = ConsumerFactory.getRMQPullConsumer(namesrvAddr, groupId, rpcHook);
        consumer.startDefaultPull();
        VerifyUtils.tryReceiveOnce(consumer.getPullConsumer(), topic, tag, 32);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < sendNum; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.send(message);
        }
        Assertions.assertEquals(sendNum, producer.getEnqueueMessages().getDataSize(), "send message failed");

        Set<MessageQueue> receiveMessageQueues = null;
        try {
            receiveMessageQueues = consumer.getPullConsumer().fetchSubscribeMessageQueues(topic);
        } catch (MQClientException e) {
            Assertions.fail("Fail to fetchSubscribeMessageQueues");
        }

        Set<MessageQueue> finalMessageQueues = receiveMessageQueues;
        CompletableFuture[] futures = new CompletableFuture[receiveMessageQueues.size()];
        Map<String, MessageExt> recvMsgs = new ConcurrentHashMap<>();
        int mqCount = 0;
        for (MessageQueue mq : finalMessageQueues) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = consumer.getPullConsumer().fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        return null;
                    boolean shouldContinue = true;
                    while (shouldContinue) {
                        PullResult pullResult = consumer.getPullConsumer().pull(mq, tag, offset, 50);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> messages = pullResult.getMsgFoundList();
                                Assertions.assertTrue(messages.size() <= 32);
                                for (MessageExt message : messages) {
                                    log.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())),
                                            message.getProperties(), message.getReconsumeTimes());
                                    if (recvMsgs.containsKey(message.getMsgId())) {
                                        Assertions.fail("Consume an ack message: " + message.getMsgId());
                                    } else {
                                        recvMsgs.put(message.getMsgId(), message);
                                    }
                                }
                                offset = pullResult.getNextBeginOffset();
                                consumer.getPullConsumer().updateConsumeOffset(mq, offset);
                                break;
                            case NO_MATCHED_MSG:
                                shouldContinue = false;
                                break;
                            case NO_NEW_MSG:
                                shouldContinue = false;
                                break;
                            case OFFSET_ILLEGAL:
                                shouldContinue = false;
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
        DataCollector dequeueMessages = DataCollectorManager.getInstance()
                .fetchListDataCollector(RandomUtils.getStringByUUID());
        for (MessageExt messageExt : recvMsgs.values()) {
            dequeueMessages.addData(messageExt);
        }
        log.info("{} messages are received", dequeueMessages.getDataSize());
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), dequeueMessages, 10);
    }

    @Test
    @DisplayName("Twenty ordinary messages are sent synchronously, and receive(50) messages are received in batch. All the pulled messages are ack() messages except the last one. expected the ack messages will not be consumed repeatedly")
    public void testNormal_simple_receive_multi_nack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        int sendNum = 20;
        RMQNormalConsumer consumer = ConsumerFactory.getRMQPullConsumer(namesrvAddr, groupId, rpcHook);
        consumer.startDefaultPull();
        VerifyUtils.tryReceiveOnce(consumer.getPullConsumer(), topic, tag, 32);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < sendNum; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            producer.send(message);
        }
        Assertions.assertEquals(sendNum, producer.getEnqueueMessages().getDataSize(), "send message failed");

        Set<MessageQueue> receiveMessageQueues = null;
        try {
            receiveMessageQueues = consumer.getPullConsumer().fetchSubscribeMessageQueues(topic);
        } catch (MQClientException e) {
            Assertions.fail("Fail to fetchSubscribeMessageQueues");
        }

        Map<String, MessageExt> recvMsgs = new ConcurrentHashMap<>();
        Set<String> unconsumedMsgIds = new HashSet<>();
        boolean[] flag = { true };
        Set<MessageQueue> finalMessageQueues = receiveMessageQueues;
        CompletableFuture[] futures = new CompletableFuture[receiveMessageQueues.size()];
        int mqCount = 0;
        for (MessageQueue mq : finalMessageQueues) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = consumer.getPullConsumer().fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        return null;
                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() < startTime + 40000) {
                        PullResult pullResult = consumer.getPullConsumer().pull(mq, tag, offset, 50);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> messages = pullResult.getMsgFoundList();
                                Assertions.assertTrue(messages.size() <= 32);
                                for (MessageExt message : messages) {
                                    log.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())),
                                            message.getProperties(), message.getReconsumeTimes());
                                    int msgId = Integer.parseInt(String.valueOf(
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody()))));
                                    if (msgId == 19 && flag[0]) {
                                        flag[0] = false;
                                        unconsumedMsgIds.add(message.getMsgId());
                                        log.info("nack message:{} {}",
                                                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())),
                                                message);
                                    } else {
                                        if (msgId == 19) {
                                            unconsumedMsgIds.add(message.getMsgId());
                                        } else {
                                            consumer.getPullConsumer().updateConsumeOffset(mq, ++offset);
                                            if (recvMsgs.containsKey(message.getMsgId())) {
                                                Assertions.fail("Consume an ack message");
                                            } else {
                                                recvMsgs.put(message.getMsgId(), message);
                                            }
                                        }
                                    }
                                }
                                if (messages.size() != 1 || Integer.parseInt(String.valueOf(StandardCharsets.UTF_8
                                        .decode(ByteBuffer.wrap(messages.get(0).getBody())))) != 19) {
                                    return null;
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
                        if (recvMsgs.size() == sendNum) {
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

        DataCollector dequeueMessages = DataCollectorManager.getInstance()
                .fetchListDataCollector(RandomUtils.getStringByUUID());
        for (MessageExt messageExt : recvMsgs.values()) {
            dequeueMessages.addData(messageExt);
        }

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), dequeueMessages, unconsumedMsgIds, 10);

    }

}
