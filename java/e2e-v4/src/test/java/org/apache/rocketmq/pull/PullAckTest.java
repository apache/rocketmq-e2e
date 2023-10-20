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

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TESTSET.NORMAL)
@Tag(TESTSET.PULL)
public class PullAckTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(PullAckTest.class);
    private String tag;
    private final static int SEND_NUM = 20;
    private RMQNormalProducer producer;

    @BeforeAll
    public static void setUpAll() {
    }

    @BeforeEach
    public void setUp() {
        tag = NameUtils.getRandomTagName();
        log.info("tag:{}", tag);
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.shutdown();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("Send 20 normal messages synchronously and expect lite pull consumer to consume with receive and ack messages successful")
    public void testNormal_lite_pull_receive_ack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer pullConsumer = ConsumerFactory.getRMQLitePullConsumer(namesrvAddr, groupId, rpcHook, 1);
        pullConsumer.startLitePullAssignMode();
        VerifyUtils.tryReceiveOnce(pullConsumer.getLitePullConsumer());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, tag + "-" + i);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitLitePullReceiveThenAck(producer, pullConsumer.getLitePullConsumer(), topic, tag);
    }

    @Test
    @Timeout(180)
    @DisplayName("Send 20 normal messages synchronously and expect pull consumer to consume with receive and ack messages successful")
    public void testNormal_pull_receive_ack() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();

        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        RMQNormalConsumer consumer = ConsumerFactory.getRMQPullConsumer(namesrvAddr, groupId, rpcHook);
        consumer.startDefaultPull();
        VerifyUtils.tryReceiveOnce(consumer.getPullConsumer(), topic, tag, 32);
        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildNormalMessage(topic, tag, tag + "-" + i);
            producer.send(message);
        }
        TestUtils.waitForSeconds(1);
        Assertions.assertEquals(SEND_NUM, producer.getEnqueueMessages().getDataSize(), "send message failed");
        VerifyUtils.waitPullReceiveThenAck(producer, consumer.getPullConsumer(), topic, tag, 32);
    }

}
