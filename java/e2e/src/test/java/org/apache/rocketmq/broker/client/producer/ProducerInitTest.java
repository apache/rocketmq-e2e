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

package org.apache.rocketmq.broker.client.producer;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ClientConfigurationFactory;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.frame.BaseOperate;
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

@Tag(TESTSET.CLIENT)
public class ProducerInitTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(ProducerInitTest.class);
    private static String topic;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    @DisplayName("Producer is normally set,expected success")
    public void testNormalSetting() {
        try {
            provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic)
                .setMaxAttempts(3)
                .build();
        } catch (ClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Disabled
    @DisplayName("The AccessKey setting of the Producer failed, expect ONSClientException to throw")
    public void testErrorAK() {
        StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider("errorAk", account.getSecretKey());
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(account.getEndpoint())
            .setCredentialProvider(staticSessionCredentialsProvider)
            .build();

        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build();
            Message message = MessageFactory.buildMessage(topic);
            producer.send(message);
        }, "Expected ClientException to throw, but it didn't");
    }

    @Disabled
    @DisplayName("The SecretKey setting of the Producer failed, expect ONSClientException to throw")
    public void testErrorSK() {
        StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(account.getAccessKey(), "errorSk");
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(account.getEndpoint())
            .setCredentialProvider(staticSessionCredentialsProvider)
            .build();

        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build();
            Message message = MessageFactory.buildMessage(topic);
            producer.send(message);
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The NAMESRV_ADDR setting of the Producer failed, expect ONSClientException to throw")
    public void testErrorNameSrvAddr() {
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("https://www.aliyun.com")
            .build();

        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer does not set the AccessKey, expect an exception occurs when the client start")
    public void testUnsetAK() {
        assertThrows(Exception.class, () -> {
            StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(null, account.getSecretKey());
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(account.getEndpoint())
                .setCredentialProvider(staticSessionCredentialsProvider)
                .build();
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer does not set the SecretKey, expect an exception occurs when the client start")
    public void testUnsetSK() {
        assertThrows(Exception.class, () -> {
            StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(account.getAccessKey(), null);
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(account.getEndpoint())
                .setCredentialProvider(staticSessionCredentialsProvider)
                .build();
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer does not set the Properties, expect an exception occurs when the client start")
    public void testUnsetProperties() {
        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(null)
                .setTopics(topic)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer sets the maximum retry times to 0, expect the client start success")
    public void testSet0MaxAttempts() {
        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic)
                .setMaxAttempts(0)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer sets the maximum retry times to -1, expect the client start failed")
    public void testSetLess0MaxAttempts() {
        assertThrows(Exception.class, () -> {
            provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setMaxAttempts(-1)
                .setTopics(topic)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer sets the maximum retry times to 100, expect the client start failed")
    public void testSet100MaxAttempts() {
        assertThrows(Exception.class, () -> {
            provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setMaxAttempts(-1)
                .setTopics(topic)
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer sets a non-existent topic, expect the client start failed")
    public void testSetNotExistTopic() {
        assertThrows(Exception.class, () -> {
            provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setMaxAttempts(3)
                .setTopics("notExistTopic")
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer sets the same topic, expect the client start failed")
    public void testSetTwoTopic() {
        try {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic, topic)
                .build();
        } catch (ClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Test
    @DisplayName("The Producer sets more topics, one of which does not exist, expect the client start failed")
    public void testSetTwoTopicWithOneNotExist() {
        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic, "notExistTopic")
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("ClientConfiguration is not set, expect the Producer NPE start failed")
    public void testNoClientConfiguration() {
        assertThrows(Exception.class, () -> {
            Producer producer = provider.newProducerBuilder()
                .setTopics(topic, "notExistTopic")
                .build();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The topic of Producer is not set, expected success")
    public void testNoTopics() {
        try {
            provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .build();
        } catch (Exception e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }
}
