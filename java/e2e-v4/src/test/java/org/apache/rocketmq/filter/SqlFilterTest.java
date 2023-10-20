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

package org.apache.rocketmq.filter;

import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.TestUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@Tag(TESTSET.SQL)
public class SqlFilterTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(SqlFilterTest.class);
    private final static int SEND_NUM = 10;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer pullConsumer;

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.shutdown();
        }
        if (pushConsumer != null) {
            pushConsumer.shutdown();
        }
        if (pullConsumer != null) {
            pullConsumer.shutdown();
        }
    }

    @Test
    @DisplayName("10 messages are sent synchronously, without any attribute filtering, and expected to be consumed to the 10 messages sent")
    public void testSendWithTagAndPropsRecvWithOutFilter() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("regionId", "cn-hangzhou");
        userProps.put("price", "30");
        String subExpression = "TRUE";

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.bySql(subExpression), new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps);
            producer.send(message);
        }
        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Send 10 messages with the attribute price=10 and 10 messages with the attribute price=30. Set the filtering rule to price>20 and expect only 10 messages to be consumed")
    public void testSqlSendTwoProps_SubFilterOne() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps1 = new HashMap<>();
        userProps1.put("price", "10");
        HashMap<String, String> userProps2 = new HashMap<>();
        userProps2.put("price", "30");
        String subExpression = "price>20";

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.bySql(subExpression), new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps1);
            producer.send(message);
        }
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps2);
            producer.send(message);
        }
        VerifyUtils.verifyNormalMessageWithUserProperties(producer.getEnqueueMessages(),
                pushConsumer.getListener().getDequeueMessages(), userProps1, 10);
    }

    @Test
    @DisplayName("Send 10 messages synchronously, using the attribute between{a,b} filter, expect to consume 10 messages sent")
    public void testSendWithTagAndPropsRecvWithBetween() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("regionId", "cn-hangzhou");
        userProps.put("price", "30");
        String subExpression = "(price BETWEEN 10 AND 100) AND regionId IS NOT NUll";

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.bySql(subExpression), new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps);
            producer.send(message);
        }

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
    }

    @Test
    @DisplayName("Send 10 messages synchronously, filter messages using unknown attributes, expect to consume up to 0 messages")
    public void testSendWithTagAndPropsRecvWithUnknownProps() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("regionId", "cn-hangzhou");
        userProps.put("price", "30");
        String subExpression = "product = 'MQ'";

        pushConsumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId, rpcHook);
        pushConsumer.subscribeAndStart(topic, MessageSelector.bySql(subExpression), new RMQNormalListener());

        producer = ProducerFactory.getRMQProducer(namesrvAddr, rpcHook);
        Assertions.assertNotNull(producer, "Get producer failed");

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps);
            producer.send(message);
        }
        TestUtils.waitForSeconds(20);
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
    }

}
