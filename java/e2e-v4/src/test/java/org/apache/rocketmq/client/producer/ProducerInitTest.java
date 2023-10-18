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

package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.AclClient;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.CLIENT)
public class ProducerInitTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(ProducerInitTest.class);
    private static String topic;

    @BeforeAll
    public static void setUpAll() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(methodName);
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
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.start();
            producer.shutdown();
        } catch (MQClientException e) {
            Assertions.fail("Send message failed, expected success, message:" + e.getMessage());
        }
    }

    @Disabled
    @DisplayName("The NAMESRV_ADDR setting of the Producer failed, expect ONSClientException to throw")
    public void testErrorNameSrvAddr() {
        assertThrows(Exception.class, () -> {
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
            producer.setNamesrvAddr("https://www.aliyun.com");
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.start();
            producer.shutdown();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer does not set the AccessKey, expect an exception occurs when the client start")
    public void testUnsetAK() {
        assertThrows(Exception.class, () -> {
            RPCHook aclRPCHook = AclClient.getAclRPCHook(null, account.getSecretKey());
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), aclRPCHook);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.start();
            producer.shutdown();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer does not set the SecretKey, expect an exception occurs when the client start")
    public void testUnsetSK() {
        assertThrows(Exception.class, () -> {
            RPCHook aclRPCHook = AclClient.getAclRPCHook(account.getAccessKey(), "");
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), aclRPCHook);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.start();
            producer.shutdown();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Disabled
    @DisplayName("The Producer does not set the Properties, expect an exception occurs when the client start")
    public void testUnsetProperties() {
        assertThrows(Exception.class, () -> {
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), (RPCHook) null);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.start();
            producer.shutdown();
        }, "Expected ClientException to throw, but it didn't");
    }

    @Test
    @DisplayName("The Producer sets the maximum retry times to 0, expect the client start success")
    public void testSet0MaxAttempts() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.setRetryTimesWhenSendFailed(0);
            producer.start();
            producer.shutdown();
        } catch (Exception e) {
            Assertions.fail("Expected the client to start successfully, but it didn't");
        }
    }

    @Disabled
    @DisplayName("The Producer sets the maximum retry times to -1, expect the client start failed")
    public void testSetLess0MaxAttempts() {
        assertThrows(Exception.class, () -> {
            DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID(), rpcHook);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setInstanceName(UUID.randomUUID().toString());
            producer.setRetryTimesWhenSendFailed(-1);
            producer.start();
            producer.shutdown();
        }, "Expected ClientException to throw, but it didn't");
    }
}
