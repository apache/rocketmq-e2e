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

package org.apache.rocketmq.broker.server;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.client.trans.LocalTransactionCheckerImpl;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.RandomUtils;
import org.apache.rocketmq.util.TestUtils;
import org.apache.rocketmq.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Tag(TESTSET.TRANSACTION)
@Tag(TESTSET.SMOKE)
public class TransactionMessageTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(TransactionMessageTest.class);
    private String tag;
    private final static int SEND_NUM = 10;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer simpleConsumer;

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (pushConsumer != null) {
            pushConsumer.close();
        }
        if (simpleConsumer != null) {
            simpleConsumer.close();
        }
    }

    @Test
    @DisplayName("Send 10 transaction messages and synchronously commit the transaction (Checker performs rollback), expecting those 10 messages to be consumed via PushConsumer")
    public void testTrans_SendCommit_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQTransProducer(account, topic, new LocalTransactionCheckerImpl(TransactionResolution.ROLLBACK));
        Assertions.assertNotNull(producer);
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.sendTrans(message, true);
        }
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Send 10 transaction messages and rollback directly (Checker does commit), expecting that these 10 messages cannot be consumed by PushConsumer")
    public void testTrans_SendRollback_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQTransProducer(account, topic, new LocalTransactionCheckerImpl(TransactionResolution.COMMIT));
        Assertions.assertNotNull(producer);
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.sendTrans(message, false);
        }
        //Wait for the callback, expecting not to commit the already rolled back message
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
    }

    @Test
    @DisplayName("Send 10 transaction messages and COMMIT the transaction by Checker (perform COMMIT), expecting the 10 messages to be consumed by PushConsumer")
    public void testTrans_SendCheckerCommit_PushConsume() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQTransProducer(account, topic, new LocalTransactionCheckerImpl(TransactionResolution.COMMIT));
        Assertions.assertNotNull(producer);
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.sendTrans(message, null);
        }
        //Wait for the callback to execute commit
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Send 10 transaction messages and roll back the transaction by Checker (performing ROLLBACK), expecting that the 10 messages will not be consumed by PushConsumer")
    public void testTrans_CheckerRollback() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQTransProducer(account, topic, new LocalTransactionCheckerImpl(TransactionResolution.ROLLBACK));
        Assertions.assertNotNull(producer);
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            producer.sendTrans(message, null);
        }
        //Wait for the rollback and execute rollback
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
    }

    @Test
    @DisplayName("Send 10 transactional messages and commit them by checking back (Checker commits for partial messages), and the expected committed messages can be consumed by PushConsumer")
    public void testTrans_SendCheckerPartionCommit() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(TopicMessageType.TRANSACTION.getValue(), methodName);
        String groupId = getGroupId(methodName);

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(tag), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        AtomicInteger commitMsgNum = new AtomicInteger(0);
        AtomicInteger rollbackMsgNum = new AtomicInteger(0);
        producer = ProducerFactory.getRMQTransProducer(account, topic, new TransactionChecker() {
            @Override
            public TransactionResolution check(MessageView messageView) {
                if (Integer.parseInt(StandardCharsets.UTF_8.decode(messageView.getBody()).toString()) % 2 == 0) {
                    commitMsgNum.getAndIncrement();
                    return TransactionResolution.COMMIT;
                } else {
                    rollbackMsgNum.getAndIncrement();
                    return TransactionResolution.ROLLBACK;
                }
            }
        });
        Assertions.assertNotNull(producer);

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessage(topic, tag, String.valueOf(i));
            producer.sendTrans(message, null);
        }
        await().atMost(90, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return rollbackMsgNum.get() == commitMsgNum.get() && commitMsgNum.get() == SEND_NUM / 2;
            }
        });
        //Wait for the rollback and execute commit/rollback
        TestUtils.waitForSeconds(60);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        Assertions.assertEquals(5, pushConsumer.getListener().getDequeueMessages().getDataSize());
    }

}

