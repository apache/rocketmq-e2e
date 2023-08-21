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

#include <gtest/gtest.h>
#include <spdlog/logger.h>

#include "enums/MessageType.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/NameUtils.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "factory/MessageFactory.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

TEST(DelayMessageTest, testDelay_Send_PushConsume){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::DELAY, "testDelay_Send_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testDelay_Send_PushConsume");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildDelayMessage(topic,tag,RandomUtils::getStringByUUID(),3);
        producer->send(message);
    }

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::verifyDelayMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages()),3));

    pushConsumer->shutdown();
    pullConsumer->shutdown();
    producer->shutdown();
}

////TEST(DelayMessageTest, testDelay_SendAsync_PushConsume){
////    int SEND_NUM = 10;
////    std::string topic = getTopic(MessageType::DELAY, "testDelay_SendAsync_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////    std::string group = getGroupId("testDelay_SendAsync_PushConsume");
////    std::string tag = NameUtils::getRandomTagName();
////
////    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());
////
////    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
////
////    std::this_thread::sleep_for(std::chrono::seconds(2));
////
////    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
////
////    auto producer = ProducerFactory::getRMQProducer(group);
////
////    ASSERT_NE(producer, nullptr);
////
////    for(int i=0;i<SEND_NUM;i++){
////        auto message = MessageFactory::buildDelayMessage(topic,tag,RandomUtils::getStringByUUID(),3);
////        producer->sendAsync(message);
////    }
////
////    pushConsumer->shutdown();
////    pullConsumer->shutdown();
////    producer->shutdown();
////}

////TEST(DelayMessageTest, testDelayTime15SecondsAgo){
////    int SEND_NUM = 10;
////    std::string topic = getTopic(MessageType::DELAY, "testDelayTime15SecondsAgo", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////    std::string group = getGroupId("testDelayTime15SecondsAgo");
////    std::string tag = NameUtils::getRandomTagName();
////
////    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());
////
////    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
////
////    std::this_thread::sleep_for(std::chrono::seconds(2));
////
////    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
////
////    auto producer = ProducerFactory::getRMQProducer(group);
////
////    ASSERT_NE(producer, nullptr);
////
////    pushConsumer->shutdown();
////    pullConsumer->shutdown();
////    producer->shutdown();
////}

////TEST(DelayMessageTest, testDelayTime24hAfter){
////    int SEND_NUM = 10;
////    std::string topic = getTopic(MessageType::DELAY, "testDelayTime24hAfter", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////    std::string group = getGroupId("testDelayTime24hAfter");
////    std::string tag = NameUtils::getRandomTagName();
////
////    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());
////
////    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
////
////    std::this_thread::sleep_for(std::chrono::seconds(2));
////
////    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
////
////    auto producer = ProducerFactory::getRMQProducer(group);
////
////    ASSERT_NE(producer, nullptr);
////
////    auto message = MessageFactory::buildDelayMessage(topic,tag,RandomUtils::getStringByUUID(),19);
////    
////    ASSERT_THROW(producer->send(message), std::exception);
////
////    pushConsumer->shutdown();
////    pullConsumer->shutdown();
////    producer->shutdown();
////}