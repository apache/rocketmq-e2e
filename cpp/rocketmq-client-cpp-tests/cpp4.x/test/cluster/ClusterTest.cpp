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
#include <iostream>
#include <memory>
#include <string>
#include <mutex>
#include <chrono>
#include <thread>
#include <spdlog/spdlog.h>
#include <rocketmq/MQMessage.h>
#include <rocketmq/DefaultMQProducer.h>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <rocketmq/DefaultMQPullConsumer.h>
#include "frame/BaseOperate.h"
#include "listener/MsgListener.h"
#include "listener/rmq/RMQNormalListener.h"
#include "resource/Resource.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "client/rmq/RMQNormalProducer.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

// TEST(ClusterTest, testClusterConsume){
//     std::string topic = getTopic(MessageType::NORMAL, "testClusterConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
//     std::string group1 = getGroupId("testClusterConsume1");
//     std::string group2 = getGroupId("testClusterConsume2");
//     std::string group3 = getGroupId("testClusterConsume3"); 
//     ASSERT_NO_THROW({
//         std::shared_ptr<RMQNormalListener> listener1 = std::make_shared<RMQNormalListener>("Listener1");
//         std::shared_ptr<RMQNormalListener> listener2 = std::make_shared<RMQNormalListener>("Listener2");
//         std::shared_ptr<RMQNormalListener> listener3 = std::make_shared<RMQNormalListener>("Listener3");
//         auto pushConsumer1 = ConsumerFactory::getPushConsumer(topic,group1,"*",listener1);
//         auto pushConsumer2 = ConsumerFactory::getPushConsumer(topic,group2,"*",listener2);
//         auto pushConsumer3 = ConsumerFactory::getPushConsumer(topic,group3,"*",listener3);

//         auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group1);
//         std::this_thread::sleep_for(std::chrono::seconds(5));
        
//         ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,"*",pullConsumer->getPullConsumer()));

//         auto producer = ProducerFactory::getRMQProducer(group1);

//         int count = 0;
//         for(int i = 0; i < 100; i++){
//             rocketmq::MQMessage msg(topic,"*",RandomUtils::getStringByUUID());
//             rocketmq::SendResult sendResult = producer->send(msg);
//             if(sendResult.getSendStatus() == rocketmq::SendStatus::SEND_OK){
//                 count++;
//             }
//         }

//         ASSERT_EQ(count, 100);

//         std::this_thread::sleep_for(std::chrono::seconds(5));

//         ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(listener1->getDequeueMessages())));
//         ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(listener2->getDequeueMessages())));
//         ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(listener3->getDequeueMessages())));

//         pushConsumer1->shutdown();
//         pushConsumer2->shutdown();
//         pushConsumer3->shutdown();
//         pullConsumer->shutdown();
//         producer->shutdown();
//     });
// }