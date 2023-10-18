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

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.frame.BaseOperate;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.CLIENT)
public class PullConsumerInitTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(PullConsumerInitTest.class);
    private static String topic;
    private static String groupId;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(methodName);
        groupId = getGroupId(methodName);
    }

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
    @DisplayName("PullConsumer all parameters are set properly, expect start success")
    public void testNormalSetting() {
        try {
            DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(groupId, rpcHook);
            pullConsumer.setNamesrvAddr(namesrvAddr);
            pullConsumer.subscribe(topic, "*");
            pullConsumer.setConsumerPullTimeoutMillis(10 * 1000);
            pullConsumer.start();
            pullConsumer.shutdown();
        } catch (Exception e) {
            Assertions.fail("Start [PullConsumer] failed, expected success, message: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Without setting 'Endpoint Configuration' of the consumer client, expect start failed")
    public void testNoClientConfiguration() {
        assertThrows(Exception.class, () -> {
            DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(groupId);
            pullConsumer.subscribe(topic, "*");
            pullConsumer.setConsumerPullTimeoutMillis(10 * 1000);
            pullConsumer.start();
        }, "Expected Start [PullConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("Without setting 'ConsumerGroup' of the consumer client, expect start failed")
    public void testNoConsumerGroup() {
        assertThrows(Exception.class, () -> {
            DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(rpcHook);
            pullConsumer.setNamesrvAddr(namesrvAddr);
            pullConsumer.subscribe(topic, "*");
            pullConsumer.setConsumerPullTimeoutMillis(10 * 1000);
            pullConsumer.start();
        }, "Expected Start [PullConsumer] Exception to throw, but it didn't");
    }

    @Disabled
    @DisplayName("Without setting 'Subscription' of the consumer client, expect start failed")
    public void testNoSubscription() {
        assertThrows(Exception.class, () -> {
            DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(groupId, rpcHook);
            pullConsumer.setNamesrvAddr(namesrvAddr);
            pullConsumer.setConsumerPullTimeoutMillis(10 * 1000);
            pullConsumer.start();
        }, "Expected Start [PullConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("Error setting 'SubscriptionExpressions' empty of the consumer client, except start failed")
    public void testEmptySubscription() {
        assertThrows(Exception.class, () -> {
            DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(groupId, rpcHook);
            pullConsumer.setNamesrvAddr(namesrvAddr);
            String var1 = null;
            String var2 = null;
            pullConsumer.subscribe(var1, var2);
            pullConsumer.start();
        }, "Expected Start [PullConsumer] Exception to throw, but it didn't");
    }

    @Disabled
    @DisplayName("Error setting 'ConsumerPullTimeoutMillis=0' of the consumer client, except start failed")
    public void testConsumerPullTimeoutMillisIs0s() {
        DefaultLitePullConsumer pullConsumer = null;
        try {
            pullConsumer = new DefaultLitePullConsumer(groupId, rpcHook);
            pullConsumer.setNamesrvAddr(namesrvAddr);
            pullConsumer.subscribe(topic, "*");
            pullConsumer.setConsumerPullTimeoutMillis(0 * 1000);
            pullConsumer.start();
            pullConsumer.poll();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("PullConsumer start failed");
        }
    }

    @Test
    @DisplayName("Setting 'Wait Duration = 10s',Expect the empty pull message request to return between 10s and 20s")
    public void testAwaitDuration() {
        DefaultLitePullConsumer pullConsumer = null;
        try {
            pullConsumer = new DefaultLitePullConsumer(groupId, rpcHook);
            pullConsumer.setNamesrvAddr(namesrvAddr);
            pullConsumer.subscribe(topic, "*");
            pullConsumer.setConsumerPullTimeoutMillis(10 * 1000);
            pullConsumer.start();
            long startTime = System.currentTimeMillis();
            log.info("startTime: {}", startTime);
            pullConsumer.poll(10 * 1000);
            long endTime = System.currentTimeMillis();
            log.info("endTime: {}", endTime);
            pullConsumer.shutdown();
            Assertions.assertTrue((endTime - startTime) > 10000 && (endTime - startTime) < 20000,
                    String.format("invoke method 'receive()' exception, startTime:%s, endTime:%s, intervalTime:%s",
                            startTime, endTime, endTime - startTime));
        } catch (Exception e) {
            Assertions.fail("PullConsumer start exception");
        }
    }
}
