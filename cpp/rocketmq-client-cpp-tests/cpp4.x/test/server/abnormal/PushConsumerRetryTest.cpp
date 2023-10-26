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
#include <chrono>
#include <iostream>
#include <cassert>
#include <string>
#include "gtest/gtest.h"
#include "spdlog/logger.h"
#include "rocketmq/MQMessageExt.h"
#include "rocketmq/DefaultMQPushConsumer.h"
#include "rocketmq/MQMessageListener.h"
#include "enums/MessageType.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/NameUtils.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "utils/SimpleConcurrentHashMapUtils.h"
#include "utils/SimpleConcurrentVectorUtils.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "factory/MessageFactory.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class ExceptionMsgListener : public rocketmq::MessageListenerConcurrently {
public:
    SimpleConcurrentHashMap<std::string, rocketmq::MQMessageExt> firstMsgs;
    SimpleConcurrentHashMap<std::string, rocketmq::MQMessageExt> retryMsgs;
    ExceptionMsgListener() {}
    virtual ~ExceptionMsgListener() {}

  virtual rocketmq::ConsumeStatus consumeMessage(const std::vector<rocketmq::MQMessageExt>& msgs) {
    for (size_t i = 0; i < msgs.size(); ++i) {
        auto msg = msgs[i];
        if(msg.getReconsumeTimes() == 1){
            if(!retryMsgs.contains(msg.getMsgId())){
                retryMsgs.insert(msg.getMsgId(), msg);
            }
            multi_logger->info("consume success: {}",msg.getMsgId());
        }else{
            // Simulate consuming operations
            multi_logger->info("{}","Simulate consuming operations fail");
            throw std::runtime_error("Simulate consuming operations fail");
            multi_logger->info("{}","Simulate consuming operations fail end");
            if(!firstMsgs.contains(msg.getMsgId())){
                firstMsgs.insert(msg.getMsgId(), msg);
            }
            multi_logger->info("recv msg(fail) {} ", msg.getMsgId());
        }
    }
    return rocketmq::CONSUME_SUCCESS;
  }
};

// class NullMsgListener : public rocketmq::MessageListenerConcurrently {
// public:
//     SimpleConcurrentHashMap<std::string, rocketmq::MQMessageExt> firstMsgs;
//     SimpleConcurrentHashMap<std::string, rocketmq::MQMessageExt> retryMsgs;
//     NullMsgListener() {}
//     virtual ~NullMsgListener() {}

//   virtual rocketmq::ConsumeStatus consumeMessage(const std::vector<rocketmq::MQMessageExt>& msgs) {
//     for (size_t i = 0; i < msgs.size(); ++i) {
//         auto msg = msgs[i];
//         if(msg.getReconsumeTimes() == 2){
//             if(!retryMsgs.contains(msg.getMsgId())){
//                 retryMsgs.insert(msg.getMsgId(), msg);
//             }
//             multi_logger->info("consume success: {}",msg.getMsgId());
//         }else{
//             // Simulate consuming operations
//             multi_logger->info("{}","Simulate consuming operations return null");
//             if(!firstMsgs.contains(msg.getMsgId())){
//                 firstMsgs.insert(msg.getMsgId(), msg);
//             }
//             multi_logger->info("recv msg(fail) {} ", msg.getMsgId());
//             return nullptr;
//         }
//     }
//     return rocketmq::CONSUME_SUCCESS;
//   }
// };

class NormalMsgListener : public rocketmq::MessageListenerConcurrently {
public:
    SimpleConcurrentHashMap<std::string, rocketmq::MQMessageExt> firstMsgs;
    SimpleConcurrentHashMap<std::string, rocketmq::MQMessageExt> retryMsgs;
    NormalMsgListener() {}
    virtual ~NormalMsgListener() {}

  virtual rocketmq::ConsumeStatus consumeMessage(const std::vector<rocketmq::MQMessageExt>& msgs) {
    for (size_t i = 0; i < msgs.size(); ++i) {
        auto msg = msgs[i];
        if(msg.getReconsumeTimes() > 0){
            if(!retryMsgs.contains(msg.getMsgId())){
                retryMsgs.insert(msg.getMsgId(), msg);
            }
            multi_logger->info("consume success: {}",msg.getMsgId());
            return rocketmq::CONSUME_SUCCESS;
        }else{
            if(!firstMsgs.contains(msg.getMsgId())){
                firstMsgs.insert(msg.getMsgId(), msg);
            }
            multi_logger->info("recv msg(fail) {} ", msg.getMsgId());
            return rocketmq::RECONSUME_LATER;
        }
    }
    return rocketmq::CONSUME_SUCCESS;
  }
};

class FIFOMsgListener : public rocketmq::MessageListenerOrderly {
public:
    SimpleConcurrentVector<rocketmq::MQMessageExt> recvMessages;
    FIFOMsgListener() {}
    virtual ~FIFOMsgListener() {}

  virtual rocketmq::ConsumeStatus consumeMessage(const std::vector<rocketmq::MQMessageExt>& msgs) {
    for (size_t i = 0; i < msgs.size(); ++i) {
        auto msg = msgs[i];
        if(msg.getReconsumeTimes() > 0){
            multi_logger->info("consume success: {}",msg.getMsgId());
            recvMessages.push_back(msg);
            return rocketmq::CONSUME_SUCCESS;
        }else{
            multi_logger->info("recv msg(fail) {} ", msg.getMsgId());
            return rocketmq::RECONSUME_LATER;
        }
    }
    return rocketmq::CONSUME_SUCCESS;
  }
};

//Simulate pushconsumer consumption fail, expect that the original message was not received, and capture all messages after message retry
TEST(PushConsumerRetryTest, testExceptionConsumption){
    int SEND_NUM = 5;
    std::string topic = getTopic(MessageType::NORMAL, "testExceptionConsumption", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testExceptionConsumption");
    std::string tag = NameUtils::getRandomTagName();

    ExceptionMsgListener* listener = new ExceptionMsgListener();
    auto rmqPushConsumer = std::make_shared<rocketmq::DefaultMQPushConsumer>(group);
    rmqPushConsumer->setNamesrvAddr(resource->getNamesrv());
    rmqPushConsumer->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
    rmqPushConsumer->setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
    rmqPushConsumer->setConsumeThreadCount(8);
    rmqPushConsumer->subscribe(topic, tag);
    rmqPushConsumer->setMessageModel(rocketmq::MessageModel::CLUSTERING);
    rmqPushConsumer->setMaxReconsumeTimes(2);
    rmqPushConsumer->registerMessageListener(listener);
    rmqPushConsumer->start();
    
    auto producer = ProducerFactory::getRMQProducer(group);
    ASSERT_NE(producer, nullptr);
    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->send(message);
    }

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(listener->retryMsgs.size() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    ASSERT_EQ(SEND_NUM,listener->retryMsgs.size());
    ASSERT_EQ(0,listener->firstMsgs.size());
    rmqPushConsumer->shutdown();
    producer->shutdown();
}

//Simulate pushconsumer consumption return null, expect that the original message was not received, and capture all messages after message retry
//// TEST(PushConsumerRetryTest, testNullConsumption){
////     int SEND_NUM = 5;
////     std::string topic = getTopic(MessageType::NORMAL, "testNullConsumption", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////     std::string group = getGroupId("testNullConsumption");
////     std::string tag = NameUtils::getRandomTagName();
////     NullMsgListener* listener = new NullMsgListener();
////     auto rmqPushConsumer = std::make_shared<rocketmq::DefaultMQPushConsumer>(group);
////     rmqPushConsumer->setNamesrvAddr(resource->getNamesrv());
////     rmqPushConsumer->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
////     rmqPushConsumer->setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
////     rmqPushConsumer->setConsumeThreadCount(4);
////     rmqPushConsumer->subscribe(topic, tag);
////     rmqPushConsumer->setMessageModel(rocketmq::MessageModel::CLUSTERING);
////     rmqPushConsumer->setMaxReconsumeTimes(2);
////     rmqPushConsumer->registerMessageListener(listener);
////     rmqPushConsumer->start();
////     auto producer = ProducerFactory::getRMQProducer(group);
////     ASSERT_NE(producer, nullptr);
////     for(int i=0;i<SEND_NUM;i++){
////         auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
////         producer->send(message);
////     }
////     ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());
////     std::this_thread::sleep_for(std::chrono::seconds(5));
////     ASSERT_EQ(SEND_NUM,listener->retryMsgs.size());
////     ASSERT_EQ(0,listener->firstMsgs.size());
////     rmqPushConsumer->shutdown();
////     producer->shutdown();
//// }

//The normal message is sent, and after the PushConsumer retry, the retry message is expected to be consumed
TEST(PushConsumerRetryTest, testNormalTopicPushConsumerRetry){
    int SEND_NUM = 1;
    std::string topic = getTopic(MessageType::NORMAL, "testNormalTopicPushConsumerRetry", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testNormalTopicPushConsumerRetry");
    std::string tag = NameUtils::getRandomTagName();

    NormalMsgListener* listener = new NormalMsgListener();
    auto rmqPushConsumer = std::make_shared<rocketmq::DefaultMQPushConsumer>(group);
    rmqPushConsumer->setNamesrvAddr(resource->getNamesrv());
    rmqPushConsumer->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
    rmqPushConsumer->setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
    rmqPushConsumer->setConsumeThreadCount(8);
    rmqPushConsumer->subscribe(topic, tag);
    rmqPushConsumer->setMessageModel(rocketmq::MessageModel::CLUSTERING);
    rmqPushConsumer->setMaxReconsumeTimes(2);
    rmqPushConsumer->registerMessageListener(listener);
    rmqPushConsumer->start();

    auto producer = ProducerFactory::getRMQProducer(group);
    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->send(message);
    }

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(listener->retryMsgs.size() == SEND_NUM && listener->firstMsgs.size() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    for(auto& msg : producer->getEnqueueMessages()->getAllData()){
        ASSERT_TRUE(listener->firstMsgs.contains(msg) && listener->retryMsgs.contains(msg));
    }
    rmqPushConsumer->shutdown();
    producer->shutdown();
}


//The send order message, after the PushConsumer retry, is expected to consume the retry message, and the message consumption order and send order
TEST(PushConsumerRetryTest, testFiFoTopicPushConsumerRetry){
    int SEND_NUM = 5;
    std::string topic = getTopic(MessageType::NORMAL, "testFiFoTopicPushConsumerRetry", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testFiFoTopicPushConsumerRetry");
    std::string tag = NameUtils::getRandomTagName();

    FIFOMsgListener* listener = new FIFOMsgListener();
    auto rmqPushConsumer = std::make_shared<rocketmq::DefaultMQPushConsumer>(group);
    rmqPushConsumer->setNamesrvAddr(resource->getNamesrv());
    rmqPushConsumer->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
    rmqPushConsumer->setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
    rmqPushConsumer->setConsumeThreadCount(4);
    rmqPushConsumer->subscribe(topic, tag);
    rmqPushConsumer->setMessageModel(rocketmq::MessageModel::CLUSTERING);
    rmqPushConsumer->setMaxReconsumeTimes(2);
    rmqPushConsumer->registerMessageListener(listener);
    rmqPushConsumer->start();

    auto producer = ProducerFactory::getRMQProducer(group);
    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->sendOrderMessage(message,0);
    }

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(listener->recvMessages.size() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    for (int i = 0; i < SEND_NUM; i++) {
        ASSERT_EQ(std::to_string(i), listener->recvMessages[i].getBody());
    }
    rmqPushConsumer->shutdown();
    producer->shutdown();
}