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

package org.apache.rocketmq.broker.client.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ClientConfigurationFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.CLIENT)
public class SimpleConsumerInitTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumerInitTest.class);
    private static String topic;
    private static String groupId;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
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
    @DisplayName("SimpleConsumer all parameters are set properly, expect start success")
    public void testNormalSetting() {
        try {
            SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setAwaitDuration(Duration.ofSeconds(10))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();
            simpleConsumer.close();
        } catch (Exception e) {
            Assertions.fail("Start [SimpleConsumer] failed, expected success, message: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("SimpleConsumer client start without setting 'ClientConfiguration', expect throw exception")
    public void testNoClientConfiguration() {
        assertThrows(NullPointerException.class, () -> {
            SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setAwaitDuration(Duration.ofSeconds(10))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();
        }, "Expected Start [SimpleConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("SimpleConsumer client start without setting 'ConsumerGroup', expect throw exception")
    public void testNoConsumerGroup() {
        assertThrows(Exception.class, () -> {
            SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setAwaitDuration(Duration.ofSeconds(10))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();
        }, "Expected Start [SimpleConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("SimpleConsumer client start without setting 'Subscription', expect throw exception")
    public void testNoSubscription() {
        assertThrows(Exception.class, () -> {
            SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setAwaitDuration(Duration.ofSeconds(10))
                .build();
        }, "Expected Start [SimpleConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("SimpleConsumer client start without setting 'AwaitDuration', expect throw exception ")
    public void testNoAwaitDuration() {
        assertThrows(Exception.class, () -> {
            SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();
        }, "Expected Start [SimpleConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("SimpleConsumer client start setting empty 'AwaitDuration', expect throw exception ")
    public void testEmptySubscription() {
        assertThrows(Exception.class, () -> {
            SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(new HashMap<>())
                .setAwaitDuration(Duration.ofSeconds(10))
                .build();
        }, "Expected Start [SimpleConsumer] Exception to throw, but it didn't");
    }

    @Test
    @DisplayName("SimpleConsumer client start setting 'AwaitDuration=0', except invoke 'receive()' throw exception")
    public void testAwaitDurationIs0s() {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setAwaitDuration(Duration.ofSeconds(0))
                .build();
            simpleConsumer.receive(1, Duration.ofSeconds(10));
        } catch (ClientException e) {
            e.printStackTrace();
            Assertions.fail("SimpleConsumer start failed");
        }
    }

    @Test
    @DisplayName("SimpleConsumer client start setting 'MaxMessageNum=0', except throw exception")
    public void testReceiveMaxMessageNumIs0() {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setAwaitDuration(Duration.ofSeconds(15))
                .build();
            simpleConsumer.receive(0, Duration.ofSeconds(20));
            Assertions.fail("except client receive message failed, but success");
        } catch (Exception e) {
            log.info("SimpleConsumer start failed: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("SimpleConsumer client start setting 'MaxMessageNum=100000', except success")
    public void testReceiveMaxMessageNumMore100000() {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setAwaitDuration(Duration.ofSeconds(15))
                .build();
            simpleConsumer.receive(100000, Duration.ofSeconds(20));
        } catch (Exception e) {
            Assertions.fail("Start client failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("SimpleConsumer client start setting 'invisibleDuration<1000ms', except throw exception")
    public void testReceiveInvisibleDurationLess1000ms() {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setAwaitDuration(Duration.ofSeconds(15))
                .build();
            simpleConsumer.receive(1, Duration.ofMillis(9999));
            Assertions.fail("except client receive message failed, but success");
        } catch (Exception e) {
            log.info(e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("the invisibleTime is too small"), e.getMessage());
        }
    }

    @Test
    @DisplayName("SimpleConsumer client start setting 'invisibleDuration>43200000ms', except throw exception")
    public void testReceiveInvisibleDurationMore43200000ms() {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setAwaitDuration(Duration.ofSeconds(15))
                .build();
            simpleConsumer.receive(1, Duration.ofMillis(43200001));
            Assertions.fail("except client receive message failed, but success");
        } catch (Exception e) {
            log.info(e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("the invisibleTime is too large"), e.getMessage());
        }
    }

    @Test
    @DisplayName("Setting 'AwaitDuration=10s',Expect the empty pull message request to return between 10s and 20s")
    public void testAwaitDuration() {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setAwaitDuration(Duration.ofSeconds(10))
                .build();
            long startTime = System.currentTimeMillis();
            log.info("startTime: {}", startTime);
            simpleConsumer.receive(1, Duration.ofSeconds(10));
            long endTime = System.currentTimeMillis();
            log.info("endTime: {}", endTime);
            simpleConsumer.close();
            Assertions.assertTrue((endTime - startTime) > 10000 && (endTime - startTime) < 20000, String.format("invoke method 'receive()' exception, startTime:%s, endTime:%s, intervalTime:%s", startTime, endTime, endTime - startTime));
        } catch (Exception e) {
            Assertions.fail("SimpConsumer start exception");
        }
    }
}
