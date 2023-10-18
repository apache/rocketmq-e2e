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

import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
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
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;
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
public class PullOrderParamTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(PullOrderParamTest.class);
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
    @DisplayName("When sending 20 sequential messages synchronously using the same MessageQueue, PullConsumer normally receives messages, but does not ack messages, and keeps the sequence; the messages are stuck at the first")
    public void testFIFO_pull_receive_nack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQPullConsumer(namesrvAddr, groupId, rpcHook);
        consumer.startDefaultPull();
        VerifyUtils.tryReceiveOnce(consumer.getPullConsumer(), topic, tag, 32);
        RMQNormalProducer producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(topic);
        List<MessageQueue> messageGroup = new ArrayList<>();
        messageGroup.add(messageQueues.get(0));
        producer.sendWithQueue(messageGroup, tag, SEND_NUM);

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
        List<MessageExt> receivedMessage = new ArrayList<>();
        int mqCount = 0;
        for (MessageQueue mq : finalMessageQueues) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = consumer.getPullConsumer().fetchConsumeOffset(mq, false);
                    if (offset < 0)
                        return null;
                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() < startTime + 30000) {
                        PullResult pullResult = consumer.getPullConsumer().pull(mq, tag, offset, 1);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> messages = pullResult.getMsgFoundList();
                                for (MessageExt message : messages) {
                                    log.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())),
                                            message.getProperties(), message.getReconsumeTimes());
                                    sendCollection
                                            .removeIf(messageExt -> messageExt.getMsgId().equals(message.getMsgId()));
                                    receivedMessage.add(message);
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
        log.info("A total of {} messages were received", receivedMessage.size());
        for (MessageExt ext : receivedMessage) {
            if (!StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ext.getBody())).toString().equals("0")) {
                Assertions.fail(String.format("Consumption out of order, expected :Body=%s Actual :Body=%s", 0,
                        StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ext.getBody()))));
            }
        }
    }
}
