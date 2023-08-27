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
#include <string>

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

TEST(OrderMessageTest, testOrder_Send_PushConsumeOrderly){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::FIFO, "testOrder_Send_PushConsumeOrderly", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testOrder_Send_PushConsumeOrderly");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->sendOrderMessage(message,i%2);
    }

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::verifyOrderMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

    pushConsumer->shutdown();
    pullConsumer->shutdown();
    producer->shutdown();
}