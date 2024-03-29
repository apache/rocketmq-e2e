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

//Send 10 normal messages synchronously, expecting those 10 messages to be consumed through PushConsumer
TEST(NormalMessageTest, testNormal_Send_PushConsume){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testNormal_Send_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testNormal_Send_PushConsume");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->send(message);
    }

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

    pushConsumer->shutdown();
    producer->shutdown();
}

////TEST(NormalMessageTest, testNormal_SendAsync_PushConsume){
////    int SEND_NUM = 10;
////    std::string topic = getTopic(MessageType::NORMAL, "testNormal_SendAsync_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////    std::string group = getGroupId("testNormal_SendAsync_PushConsume");
////    std::string tag = NameUtils::getRandomTagName();
////
////    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());
////
////    auto producer = ProducerFactory::getRMQProducer(group);
////
////    ASSERT_NE(producer, nullptr);
////
////    for(int i=0;i<SEND_NUM;i++){
////        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
////        producer->sendAsync(message);
////    }
////
////    pushConsumer->shutdown();
////    producer->shutdown();
////}