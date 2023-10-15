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

//Send 10 messages, set other groupid's pushconsumer consumption from first, expect to accept all messages again
TEST(OffsetTest, testConsumeFromFisrtOffset){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testConsumeFromFisrtOffset", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group1 = getGroupId("testConsumeFromFisrtOffset1");
    std::string group2 = getGroupId("testConsumeFromFisrtOffset2");
    std::string tag = NameUtils::getRandomTagName();

    std::shared_ptr<RMQNormalListener> listener1 = std::make_shared<RMQNormalListener>("Listener1");
    std::shared_ptr<RMQNormalListener> listener2 = std::make_shared<RMQNormalListener>("Listener2");

    auto rmqPushConsumer1 = std::make_shared<rocketmq::DefaultMQPushConsumer>(group1);
    rmqPushConsumer1->setNamesrvAddr(resource->getNamesrv());
    rmqPushConsumer1->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
    rmqPushConsumer1->setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
    rmqPushConsumer1->setConsumeThreadCount(4);
    rmqPushConsumer1->subscribe(topic, tag);
    rmqPushConsumer1->registerMessageListener(listener1.get());
    rmqPushConsumer1->start();

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group1);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    auto producer = ProducerFactory::getRMQProducer(group1);

    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        producer->send(message);
    }

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(listener1->getDequeueMessages()->getDataSize() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    ASSERT_EQ(listener1->getDequeueMessages()->getDataSize(),SEND_NUM);

    rmqPushConsumer1->shutdown();

    multi_logger->info("first pushconsumer end");

    auto rmqPushConsumer2 = std::make_shared<rocketmq::DefaultMQPushConsumer>(group2);
    rmqPushConsumer2->setNamesrvAddr(resource->getNamesrv());
    rmqPushConsumer2->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
    rmqPushConsumer2->setConsumeFromWhere(rocketmq::CONSUME_FROM_FIRST_OFFSET);
    rmqPushConsumer2->setConsumeThreadCount(4);
    rmqPushConsumer2->subscribe(topic, tag);
    rmqPushConsumer2->registerMessageListener(listener2.get());
    rmqPushConsumer2->start();

    long endTime2 = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
    while(endTime2 > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        if(listener2->getDequeueMessages()->getDataSize() == SEND_NUM){
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    ASSERT_EQ(listener2->getDequeueMessages()->getDataSize(),SEND_NUM);


    rmqPushConsumer2->shutdown();
    producer->shutdown();
}

//send 10 messages, PullConsumer normally receives messages, but does not update messages offset, expect the messages are receive again
TEST(OffsetTest, test_pull_receive_nack){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "test_pull_receive_nack", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("test_pull_receive_nack");
    std::string tag = NameUtils::getRandomTagName();

    auto pullConsumer = ConsumerFactory::getPullConsumer(topic,group);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer));

    auto producer = ProducerFactory::getRMQProducer(group);
    ASSERT_NE(producer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,tag+"-"+std::to_string(i));
        producer->send(message);
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

    std::vector<rocketmq::MQMessageQueue> mqs;
    pullConsumer->fetchSubscribeMessageQueues(topic, mqs);

    std::vector<rocketmq::MQMessageExt> receivedMessage;
    SimpleConcurrentHashMap<std::string,std::atomic<int>> map;

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+30* 1000L;
    while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
        std::vector<std::function<void()>> runnables;
        for (auto& mq : mqs) {
            runnables.push_back([&](){
                long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                if(offset<0) return;
                rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, SEND_NUM);
                switch (pullResult.pullStatus) {
                    case rocketmq::FOUND:
                        for(int j=0;j<pullResult.msgFoundList.size();j++){
                            std::string msgId = pullResult.msgFoundList[j].getMsgId();
                            if(map.contains(msgId)){
                                map[msgId]++;
                            }else{
                                std::atomic<int> val(1);
                                map.insert(msgId,val.load());
                            }
                        }
                        break;
                    case rocketmq::NO_MATCHED_MSG:
                        break;
                    case rocketmq::NO_NEW_MSG:
                        break;
                    case rocketmq::OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            });
        }
        
        std::vector<std::future<void>> futures;
        for (const auto& runnable : runnables) {
            futures.push_back(std::async(std::launch::async, runnable));
        }

        for (auto& future : futures) {
            future.get();
        }
    }

    for(auto& value : map.getAllValues()){
        ASSERT_TRUE(value>1);
    }

    pullConsumer->shutdown();
    producer->shutdown();
}