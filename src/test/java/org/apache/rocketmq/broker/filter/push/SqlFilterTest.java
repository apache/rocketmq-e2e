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

package org.apache.rocketmq.broker.filter.push;

import java.time.Duration;
import java.util.HashMap;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
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

@Tag(TESTSET.SQL)
public class SqlFilterTest extends BaseOperate {
    private final Logger log = LoggerFactory.getLogger(SqlFilterTest.class);
    private final static int SEND_NUM = 10;
    private RMQNormalProducer producer;
    private RMQNormalConsumer pushConsumer;
    private RMQNormalConsumer simpleConsumer;

    @BeforeEach
    public void setUp() {
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
    @DisplayName("10 messages are sent synchronously, without any attribute filtering, and expected to be consumed to the 10 messages sent")
    public void testSendWithTagAndPropsRecvWithOutFilter() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("regionId", "cn-hangzhou");
        userProps.put("price", "30");
        String subExpression = "TRUE";

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

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
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps1 = new HashMap<>();
        userProps1.put("price", "10");
        HashMap<String, String> userProps2 = new HashMap<>();
        userProps2.put("price", "30");
        String subExpression = "price>20";

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps1);
            producer.send(message);
        }
        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps2);
            producer.send(message);
        }
        VerifyUtils.verifyNormalMessageWithUserProperties(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages(), userProps1, 10);
    }

    @Test
    @DisplayName("Send 10 messages synchronously, using the attribute between{a,b} filter, expect to consume 10 messages sent")
    public void testSendWithTagAndPropsRecvWithBetween() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("regionId", "cn-hangzhou");
        userProps.put("price", "30");
        String subExpression = "(price BETWEEN 10 AND 100) AND regionId IS NOT NUll";

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

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
        String topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        String groupId = getGroupId(methodName);

        HashMap<String, String> userProps = new HashMap<>();
        userProps.put("regionId", "cn-hangzhou");
        userProps.put("price", "30");
        String subExpression = "product = 'MQ'";

        pushConsumer = ConsumerFactory.getRMQPushConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), new RMQNormalListener());
        simpleConsumer = ConsumerFactory.getRMQSimpleConsumer(account, topic, groupId, new FilterExpression(subExpression, FilterExpressionType.SQL92), Duration.ofSeconds(10));
        VerifyUtils.tryReceiveOnce(simpleConsumer.getSimpleConsumer());

        producer = ProducerFactory.getRMQProducer(account, topic);
        Assertions.assertNotNull(producer);

        for (int i = 0; i < SEND_NUM; i++) {
            Message message = MessageFactory.buildMessageWithProperty(topic, userProps);
            producer.send(message);
        }
        TestUtils.waitForSeconds(20);
        Assertions.assertEquals(0, pushConsumer.getListener().getDequeueMessages().getDataSize());
    }

}

