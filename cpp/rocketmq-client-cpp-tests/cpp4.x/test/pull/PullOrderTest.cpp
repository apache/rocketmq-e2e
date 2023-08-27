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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <cassert>

#include <gtest/gtest.h>
#include <rocketmq/SendResult.h>
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

TEST(PullOrderTest, waitFIFOReceiveThenAck){
    int SEND_NUM = 20;
    std::string topic = getTopic(MessageType::FIFO, "waitFIFOReceiveThenAck", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("waitFIFOReceiveThenAck");
    std::string tag = NameUtils::getRandomTagName();

    auto pullConsumer = ConsumerFactory::getPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer));

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->send(message);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());
    //只能保证拉的队列里面的消息是有序的，不能保证整个队列的消息是有序的
    ASSERT_TRUE(VerifyUtils::waitFIFOReceiveThenAck(producer,pullConsumer,topic,tag,5));

    pullConsumer->shutdown();
    producer->shutdown();
}