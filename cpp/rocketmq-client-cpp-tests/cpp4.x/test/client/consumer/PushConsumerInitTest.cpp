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
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <rocketmq/DefaultMQProducer.h>
#include <rocketmq/MQMessageListener.h>
#include <rocketmq/MQMessage.h>
#include "resource/Resource.h"
#include "frame/BaseOperate.h"
#include "listener/MsgListener.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

TEST(PushConsumerInitTest, testNormalSetting){
    SCOPED_TRACE("Start [PushConsumer] failed, expected success.");
    std::string groupId = getGroupId("testNormalSetting");
    std::string topic = getTopic(MessageType::NORMAL, "testNormalSetting",resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
    ASSERT_NO_THROW({
        rocketmq::DefaultMQPushConsumer consumer(groupId);
        consumer.setNamesrvAddr(resource->getNamesrv());
        consumer.setConsumeThreadCount(20);
        consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
        consumer.subscribe(topic, "*");
        MsgListener msglistener;
        consumer.registerMessageListener(&msglistener); 
        consumer.start();
        std::this_thread::sleep_for(std::chrono::seconds(5));
        consumer.shutdown();
    });
}

//// TEST(PushConsumerInitTest, testErrorAK){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = getGroupId("testErrorAK");
////     std::string topic = getTopic(MessageType::NORMAL, "testErrorAK", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         consumer.setNamesrvAddr(resource->getNamesrv());
////         consumer.setSessionCredentials("errorAk", resource->getSecretKey(), "YUN");
////         consumer.setConsumeThreadCount(20);
////         consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
////         consumer.subscribe(topic, "*");
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }

//// TEST(PushConsumerInitTest, testErrorSK){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = getGroupId("testErrorSK");
////     std::string topic = getTopic(MessageType::NORMAL, "testErrorSK", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         consumer.setNamesrvAddr(resource->getNamesrv());
////         consumer.setSessionCredentials(resource->getAccessKey(), "errorAk", "YUN");
////         consumer.setConsumeThreadCount(20);
////         consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
////         consumer.subscribe(topic, "*");
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }

TEST(PushConsumerInitTest, testNormalNameserver){
    SCOPED_TRACE("Start [PushConsumer] [Producer], expected success.");
    std::string groupId = getGroupId("testNormalNameserver");
    std::string topic = getTopic(MessageType::NORMAL, "testNormalNameserver", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());

    rocketmq::DefaultMQPushConsumer consumer(groupId);
    consumer.setNamesrvAddr(resource->getNamesrv());
    consumer.subscribe(topic, "*");
    consumer.setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
    consumer.setConsumeThreadCount(4);
    MsgListener *msglistener=new MsgListener();
    consumer.registerMessageListener(msglistener);
    consumer.start();

    std::this_thread::sleep_for(std::chrono::seconds(2));

    rocketmq::DefaultMQProducer producer(groupId);
    producer.setTcpTransportTryLockTimeout(1000);
    producer.setTcpTransportConnectTimeout(400);
    producer.setNamesrvAddr(resource->getNamesrv());
    producer.start();

    int msgcount = 10;
    for (int i = 0; i < msgcount; ++i) {
        rocketmq::MQMessage msg(topic,"*",RandomUtils::getStringByUUID());
        producer.send(msg);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    ASSERT_EQ(msgcount, msglistener->getMsgCount());
    producer.shutdown();
    consumer.shutdown();
    delete msglistener;
}

////TEST(PushConsumerInitTest, testErrorNameserver){
////    SCOPED_TRACE("Error setting the 'EndPoint' of the consumer client,expect start failed.");
////    std::string groupId = getGroupId("testErrorNameserver");
////    std::string topic = "testErrorNameserver";
////    ASSERT_THROW({
////        rocketmq::DefaultMQPushConsumer consumer(groupId);
////        consumer.setNamesrvAddr("https://www.aliyun.com");
////        consumer.setConsumeThreadCount(20);
////        consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
////        consumer.subscribe(topic, "*");
////        MsgListener msglistener;
////        consumer.registerMessageListener(&msglistener); 
////        consumer.setAsyncPull(true);
////        consumer.start();
////        std::this_thread::sleep_for(std::chrono::seconds(5));
////        consumer.shutdown();
////    },rocketmq::MQException);
////}

//// TEST(PushConsumerInitTest, testErrorTopic){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = getGroupId("testErrorTopic");
////     std::string topic = "testErrorTopic";
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         consumer.setNamesrvAddr(resource->getNamesrv());
////         consumer.setConsumeThreadCount(20);
////         consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
////         consumer.subscribe("testErrorTopic", "*");
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }

//// TEST(PushConsumerInitTest, testNoGroupId){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = "123";
////     std::string topic = getTopic(MessageType::NORMAL, "testNoGroupId", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         consumer.setNamesrvAddr(resource->getNamesrv());
////         consumer.setConsumeThreadCount(20);
////         consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
////         consumer.subscribe("testErrorTopic", "*");
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }

////TEST(PushConsumerInitTest, testNoSubscription){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = "testNoSubscription";
////     std::string topic = getTopic(MessageType::NORMAL, "testNoSubscription", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         consumer.setNamesrvAddr(resource->getNamesrv());
////         consumer.setConsumeThreadCount(20);
////         consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }

////TEST(PushConsumerInitTest, testNoClientConfiguration){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = "testNoClientConfiguration";
////     std::string topic = getTopic(MessageType::NORMAL, "testNoClientConfiguration", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.subscribe(topic, "*");
////         MsgListener msglistener;
////         consumer.registerMessageListener(&msglistener); 
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }

////TEST(PushConsumerInitTest, testNoClientConfiguration){
////     SCOPED_TRACE("Expected Start [PushConsumer] ClientException to throw, but it didn't.");
////     std::string groupId = "testNoListener";
////     std::string topic = getTopic(MessageType::NORMAL, "testNoListener", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
////     ASSERT_THROW({
////         rocketmq::DefaultMQPushConsumer consumer(groupId);
////         consumer.subscribe(topic, "*");
////         consumer.start();
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         consumer.shutdown();
////     },rocketmq::MQException);
//// }