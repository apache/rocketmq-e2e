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
#include <rocketmq/MQMessageExt.h>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <rocketmq/MQMessageListener.h>

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

//Send 10 messages in batch, expect pushconsumer to accept them all
TEST(BatchProducerTest, testBatchProducer){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testBatchProducer", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testBatchProducer");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);
    
    std::vector<rocketmq::MQMessage> msgs;
    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        msgs.push_back(message);
    }

    ASSERT_NO_THROW({
        rocketmq::SendResult sendResult= producer->getProducer()->send(msgs);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SEND_OK);
    });

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(pushConsumer->getListener()->getDequeueMessages()->getDataSize() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    ASSERT_EQ(pushConsumer->getListener()->getDequeueMessages()->getDataSize(),SEND_NUM);

    pushConsumer->shutdown();
    producer->shutdown();
}

//std::vector<MQMessage>& msgs, const MQMessageQueue& mq
//Send 10 messages to a queue in batch , expect pushconsumer to accept them all
TEST(BatchProducerTest, testBatchProducer_queue){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testBatchProducer_queue", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testBatchProducer_queue");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
    std::vector<rocketmq::MQMessageQueue> mqs;
    pullConsumer->getPullConsumer()->fetchSubscribeMessageQueues(topic, mqs);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);
    
    std::vector<rocketmq::MQMessage> msgs;
    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        msgs.push_back(message);
    }

    ASSERT_NO_THROW({
        rocketmq::SendResult sendResult= producer->getProducer()->send(msgs,mqs[0]);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SEND_OK);
    });

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(pushConsumer->getListener()->getDequeueMessages()->getDataSize() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    ASSERT_EQ(pushConsumer->getListener()->getDequeueMessages()->getDataSize(),SEND_NUM);

    pushConsumer->shutdown();
    producer->shutdown();
}