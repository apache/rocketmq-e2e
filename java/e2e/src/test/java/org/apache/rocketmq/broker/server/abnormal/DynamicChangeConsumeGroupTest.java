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

//package org.apache.rocketmq.broker.server.abnormal;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.concurrent.Callable;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import org.apache.rocketmq.client.apis.ClientException;
//import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
//import org.apache.rocketmq.client.apis.consumer.FilterExpression;
//import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
//import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
//import org.apache.rocketmq.client.apis.message.Message;
//import org.apache.rocketmq.client.apis.producer.Producer;
//import org.apache.rocketmq.client.apis.producer.SendReceipt;
//import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
//import org.apache.rocketmq.client.rmq.RMQNormalProducer;
//import org.apache.rocketmq.console.enums.consumer.group.ConsumeRetryPolicyEnum;
//import org.apache.rocketmq.console.enums.consumer.group.ConsumerDeliveryOrderTypeEnum;
//import org.apache.rocketmq.console.enums.consumer.group.ConsumerGroupStatusEnum;
//import org.apache.rocketmq.console.enums.topic.MessageTypeEnum;
//import org.apache.rocketmq.enums.TESTSET;
//import org.apache.rocketmq.factory.ClientConfigurationFactory;
//import org.apache.rocketmq.factory.ConsumerFactory;
//import org.apache.rocketmq.factory.MessageFactory;
//import org.apache.rocketmq.factory.ProducerFactory;
//import org.apache.rocketmq.frame.BaseOperate;
//import org.apache.rocketmq.listener.rmq.RMQNormalListener;
//import org.apache.rocketmq.util.NameUtils;
//import org.apache.rocketmq.util.RandomUtils;
//import org.apache.rocketmq.util.TestUtils;
//import org.apache.rocketmq.util.VerifyUtils;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Tag;
//import org.junit.jupiter.api.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static java.util.concurrent.TimeUnit.SECONDS;
//import static org.awaitility.Awaitility.await;
//
//@Tag(TESTSET.CLIENT)
//@Tag(TESTSET.SMOKE)
//public class DynamicChangeConsumeGroupTest extends BaseOperate {
//    private final Logger logger = LoggerFactory.getLogger(DynamicChangeConsumeGroupTest.class);
//    private String tag;
//    private final static int sendNum = 10;
//    private String Concurrently = ConsumerDeliveryOrderTypeEnum.CONCURRENTLY.getCode();
//    private String Orderly = ConsumerDeliveryOrderTypeEnum.ORDERLY.getCode();
//    private String DefaultRetryPolicy = ConsumeRetryPolicyEnum.CONSUME_RETRY_POLICY_DEFAULT.getCode();
//    private String FixedRetryPolicy = ConsumeRetryPolicyEnum.CONSUME_RETRY_POLICY_FIXED.getCode();
//
//    @BeforeEach
//    public void setUp() {
//        tag = NameUtils.getRandomTagName();
//    }
//
//    @AfterEach
//    public void tearDown() {
//    }
//
//    @Test
//    @DisplayName("Sends a common message to check whether the consumption times match the ConsumeGroup retry times")
//    public void testNormalTopicDynamicChangeRetryTimes() {
//        String className = this.getClass().getName();
//        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
//        String topic = getTopic(MessageTypeEnum.NORMAL.getCode(), className, methodName);
//        String groupId = NameUtils.getRandomGroupName(methodName);
//        try {
//            int maxRetryTimes = 1;
//            groupWrapper.createConsumerGroup(primaryAccount1, account1InstanceId, groupId, "", Concurrently, DefaultRetryPolicy, maxRetryTimes, true);
//            groupWrapper.waitConsumerGroupExist(primaryAccount1, account1InstanceId, groupId);
//
//            Producer producer = ProducerFactory.getProducer(primaryAccount1, topic);
//            Assertions.assertNotNull(producer, "Get Producer Failed");
//            AtomicInteger consumeTimes = new AtomicInteger(0);
//            HashMap<String, Integer> checkResult = new HashMap<>();
//            AtomicBoolean beyondRetryTimes = new AtomicBoolean(false);
//            SimpleConsumer simpleConsumer = null;
//            try {
//                provider.newPushConsumerBuilder()
//                    .setClientConfiguration(ClientConfigurationFactory.build(primaryAccount1))
//                    .setConsumerGroup(groupId)
//                    .setSubscriptionExpressions(Collections.singletonMap(topic, new FilterExpression(tag, FilterExpressionType.TAG)))
//                    .setConsumptionThreadCount(20)
//                    .setMessageListener(messageView -> {
//                        consumeTimes.getAndIncrement();
//                        checkResult.put(messageView.getMessageId().toString(), consumeTimes.get());
//                        logger.info(String.format("consume message failed ,%s", messageView));
//                        return ConsumeResult.FAILURE;
//                    })
//                    .build();
//                simpleConsumer = ConsumerFactory.getSimpleConsumer(primaryAccount1, topic, groupId, new FilterExpression(tag, FilterExpressionType.TAG), Duration.ofSeconds(10));
//                VerifyUtils.tryReceiveOnce(simpleConsumer);
//            } catch (ClientException e) {
//                throw new RuntimeException(e);
//            }
//
//            Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
//            SendReceipt firstSendReceipt =  producer.send(message);
//            //The interval between a retry and the normal consumption is 10s
//            await().atMost(120, SECONDS).until(new Callable<Boolean>() {
//                @Override
//                public Boolean call() {
//                    return checkResult.get(firstSendReceipt.getMessageId().toString()) == 2;
//                }
//            });
//            //Verify that the delivery did not exceed the maximum number of retries
//            Assertions.assertEquals(2, consumeTimes.get(), "The maximum consumption retry times is abnormal. Procedure");
//
//            groupWrapper.updateConsumerGroup(primaryAccount1, account1InstanceId, groupId, "", Concurrently, DefaultRetryPolicy, maxRetryTimes + 1, true);
//            await().atMost(60, SECONDS).until(new Callable<Boolean>() {
//                @Override
//                public Boolean call() {
//                    return ConsumerGroupStatusEnum.RUNNING.getCode().equals(
//                        groupWrapper.getConsumerGroup(primaryAccount1, account1InstanceId, groupId, true).getData().getStatus());
//                }
//            });
//            //Refresh the gid cache obtained by the proxy from the metadata center
//            TestUtils.waitForSeconds(20);
//            VerifyUtils.tryReceiveOnce(simpleConsumer);
//            consumeTimes.set(0);
//            SendReceipt secondSendReceipt = producer.send(message);
//            //The interval between entering the dead letter and normal consumption is 40s
//            await().atMost(180, SECONDS).until(new Callable<Boolean>() {
//                @Override
//                public Boolean call() {
//                    return checkResult.get(secondSendReceipt.getMessageId().toString()) == 3;
//                }
//            });
//            //Verify that the delivery did not exceed the maximum number of retries
//            Assertions.assertEquals(3, consumeTimes.get(), "send message failed");
//        } catch (Exception e) {
//            Assertions.fail();
//        }
//    }
//
//    @Test
//    @DisplayName("Dynamically modify the consumeGroup consumption mode, expecting the consumeGroup to consume sequential messages Concurrently with the current Orderly or orderly Settings")
//    public void testFiFoTopicChangeDeliveryOrderType() {
//        String className = this.getClass().getName();
//        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
//        String topic = getTopic(MessageTypeEnum.FIFO.getCode(), className, methodName);
//        String groupId = NameUtils.getRandomGroupName(methodName);
//
//        try {
//            groupWrapper.createConsumerGroup(primaryAccount1, account1InstanceId, groupId, "", Concurrently, DefaultRetryPolicy, 16, true);
//            groupWrapper.waitConsumerGroupExist(primaryAccount1, account1InstanceId, groupId);
//
//            RMQNormalConsumer pushConsumer = ConsumerFactory.getRMQPushConsumer(primaryAccount1, topic, groupId, new FilterExpression(tag), new RMQNormalListener());
//            SimpleConsumer simpleConsumer = ConsumerFactory.getSimpleConsumer(primaryAccount1, topic, groupId, new FilterExpression(tag, FilterExpressionType.TAG), Duration.ofSeconds(10));
//            VerifyUtils.tryReceiveOnce(simpleConsumer);
//
//            RMQNormalProducer producer = ProducerFactory.getRMQProducer(primaryAccount1, topic);
//            Assertions.assertNotNull(producer, "Get Producer failed");
//            for (int i = 0; i < sendNum; i++) {
//                String shardingKey = Character.toString((char) (i % 2 + 'a'));
//                Message message = MessageFactory.buildOrderMessage(topic, tag, RandomUtils.getStringByUUID(), shardingKey);
//                producer.send(message);
//            }
//            Assertions.assertEquals(sendNum, producer.getEnqueueMessages().getDataSize(), "send message failed");
//            VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
//
//            groupWrapper.updateConsumerGroup(primaryAccount1, account1InstanceId, groupId, "", Orderly, FixedRetryPolicy, 16, true);
//            await().atMost(60, SECONDS).until(new Callable<Boolean>() {
//                @Override
//                public Boolean call() {
//                    return ConsumerGroupStatusEnum.RUNNING.getCode().equals(
//                        groupWrapper.getConsumerGroup(primaryAccount1, account1InstanceId, groupId, true).getData().getStatus());
//                }
//            });
//            VerifyUtils.tryReceiveOnce(simpleConsumer);
//            TestUtils.waitForSeconds(20);
//
//            producer.clearMsg();
//            pushConsumer.getListener().clearMsg();
//            for (int i = 0; i < sendNum; i++) {
//                String shardingKey = Character.toString((char) (i % 2 + 'a'));
//                Message message = MessageFactory.buildOrderMessage(topic, tag, String.valueOf(i), shardingKey);
//                producer.send(message);
//            }
//            Assertions.assertEquals(sendNum, producer.getEnqueueMessages().getDataSize(), "send message failed");
//            VerifyUtils.verifyOrderMessage(producer.getEnqueueMessages(), pushConsumer.getListener().getDequeueMessages());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//}
//
