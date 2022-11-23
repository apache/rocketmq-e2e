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

import java.util.Collections;
import java.util.HashMap;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ClientConfigurationFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.util.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PushConsumer client initialization use case
 */
@Tag(TESTSET.CLIENT)
public class PushConsumerInitTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(PushConsumerInitTest.class);
    private static String topic;
    private static String groupId;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
        groupId = getGroupId(methodName);
        TestUtils.waitForSeconds(2);
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
    @DisplayName("PushConsumer all parameters are set properly, expect start success")
    public void testNormalSetting() {
        try {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setConsumptionThreadCount(20)
                .setMaxCacheMessageCount(1000)
                .setMaxCacheMessageSizeInBytes(4 * 1024 * 1024)
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        } catch (Exception e) {
            Assertions.fail("Start [PushConsumer] failed, expected success, message: " + e.getMessage());
        }
    }

    @Disabled
    @Tag(TESTSET.ACL)
    @DisplayName("Error setting the AccessKey of the consumer client,expect start failed")
    public void testErrorAK() {
        StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider("errorAk", account.getSecretKey());
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(account.getEndpoint())
            .setCredentialProvider(staticSessionCredentialsProvider)
            .build();

        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Disabled
    @Tag(TESTSET.ACL)
    @DisplayName("Error setting the SecretKey of the consumer client,expect start failed")
    public void testErrorSK() {
        StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(account.getAccessKey(), "errorSk");
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(account.getEndpoint())
            .setCredentialProvider(staticSessionCredentialsProvider)
            .build();

        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @Tag(TESTSET.ACL)
    @DisplayName("Error setting the 'EndPoint' of the consumer client,expect start failed")
    public void testErrorNameserver() {
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("https://www.aliyun.com")
            .build();

        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("Set the consumer client's topic error, expecting a message receiving failure to throw an Exception")
    public void testErrorTopic() {
        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap("topicNotExist", FilterExpression.SUB_ALL))
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("Without setting ConsumerGroup, expect PushConsumer NPE exception to start")
    public void testNoGroupId() {
        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The 'SubscriptionExpressions' is not set, expect PushConsumer IllegalArgumentException exception to start")
    public void testNoSubscription() {
        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("Set an empty SubscriptionExpressions, expecting PushConsumer IllegalArgumentException to be raised")
    public void testEmptySubscription() {
        assertThrows(Exception.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setConsumerGroup(groupId)
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setSubscriptionExpressions(new HashMap<>())
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The 'ClientConfiguration' is not set. PushConsumer NPE exception is expected")
    public void testNoClientConfiguration() {
        assertThrows(NullPointerException.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .setMessageListener(messageView -> {
                    return ConsumeResult.SUCCESS;
                })
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The 'MessageListener' is not set. PushConsumer NPE exception is expected")
    public void testNoListener() {
        assertThrows(NullPointerException.class, () -> {
            PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(groupId)
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();
            pushConsumer.close();
        }, "Expected Start [PushConsumer] ClientException to throw, but it didn't");
    }
}
