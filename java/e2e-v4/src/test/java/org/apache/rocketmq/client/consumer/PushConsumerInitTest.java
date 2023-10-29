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

package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.TestUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PushConsumer client initialization use case
 */
@Tag(TESTSET.CLIENT)
@Tag(TESTSET.SMOKE)
public class PushConsumerInitTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(PushConsumerInitTest.class);
    private static String topic = getTopic("PullConsumerInitTest");
    @BeforeEach
    public void setUp() {

    }

    @AfterEach
    public void tearDown() {
    }

    @AfterAll
    public static void tearDownAll() {
    }

    @Test
    @DisplayName("PushConsumer all parameters are set properly, expect start success")
    public void testNormalSetting() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        try {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook,
                    new AllocateMessageQueueAveragely());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, "*");
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        } catch (Exception e) {
            Assertions.fail("Start [PushConsumer] failed, expected success, message: " + e.getMessage());
        }
    }

    @Test
    @Tag(TESTSET.ACL)
    @DisplayName("Error setting the 'EndPoint' of the consumer client,expect start failed")
    public void testErrorNameserver() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        assertThrows(Exception.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook,
                    new AllocateMessageQueueAveragely());
            pushConsumer.setNamesrvAddr("https://www.aliyun.com");
            pushConsumer.subscribe(topic, "*");
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Disabled
    @DisplayName("Set the consumer client's topic error, expecting a message receiving failure to throw an Exception")
    public void testErrorTopic() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        assertThrows(Exception.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook,
                    new AllocateMessageQueueAveragely());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe("topicNotExist", "*");
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("Without setting ConsumerGroup, expect PushConsumer NPE exception to start")
    public void testNoGroupId() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        assertThrows(Exception.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(rpcHook);
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, "*");
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Disabled
    @DisplayName("The 'Subscription' is not set, expect PushConsumer IllegalArgumentException exception to start")
    public void testNoSubscription() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        assertThrows(Exception.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook,
                    new AllocateMessageQueueAveragely());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("Set an empty Subscription, expecting PushConsumer IllegalArgumentException to be raised")
    public void testEmptySubscription() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        assertThrows(Exception.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook,
                    new AllocateMessageQueueAveragely());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            String var1 = null;
            String var2 = null;
            pushConsumer.subscribe(var1, var2);
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Disabled
    @Test
    @DisplayName("The 'Endpoint Configuration' is not set. PushConsumer IllegalState exception is expected")
    public void testNoClientConfiguration() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String groupId = getGroupId(methodName);
        assertThrows(Exception.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId);
            pushConsumer.subscribe(topic, "*");
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.setMessageListener(new RMQNormalListener());
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The 'MessageListener' is not set. PushConsumer MQClient exception is expected")
    public void testNoListener() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String topic = getTopic(methodName);
        String groupId = getGroupId(methodName);
        assertThrows(MQClientException.class, () -> {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(groupId, rpcHook,
                    new AllocateMessageQueueAveragely());
            pushConsumer.setNamesrvAddr(namesrvAddr);
            pushConsumer.subscribe(topic, "*");
            pushConsumer.setConsumeThreadMax(20);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.start();
            pushConsumer.shutdown();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }
}
