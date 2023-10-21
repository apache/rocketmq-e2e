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
#include <iostream>
#include <memory>
#include <string>
#include <mutex>
#include <chrono>
#include <thread>
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "rocketmq/MQMessage.h"
#include "rocketmq/DefaultMQProducer.h"
#include "rocketmq/DefaultMQPushConsumer.h"
#include "rocketmq/DefaultMQPullConsumer.h"
#include "frame/BaseOperate.h"
#include "listener/MsgListener.h"
#include "listener/rmq/RMQNormalListener.h"
#include "resource/Resource.h"
#include "factory/MessageFactory.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/RandomUtils.h"
#include "utils/NameUtils.h"
#include "utils/VerifyUtils.h"
#include "client/rmq/RMQNormalProducer.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

// Normal message load balancing, start 4 consumers, send 240 messages, expect 4 consumers to consume load balancing, each consume 1/4, then shutdown 2 of them, send 240 messages again, still load balancing, each consume half, and start 2 new consumers. Another 240 messages are sent, still load balanced, each consuming 1/4(bug: A segment error occurred when two pushConsumers were started for the same consumer group)
//  TEST(LoadBalancingTest, testLoadBalancing_normal_message){
//      int SEND_NUM = 240;
//      std::string topic = getTopic(MessageType::NORMAL, "testLoadBalancing_normal_message", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
//      std::string group = getGroupId("testLoadBalancing_normal_message");
//      std::string tag = NameUtils::getRandomTagName();

//     std::shared_ptr<RMQNormalListener> listener1 = std::make_shared<RMQNormalListener>("Listener1");
//     std::shared_ptr<RMQNormalListener> listener2 = std::make_shared<RMQNormalListener>("Listener2");
//     std::shared_ptr<RMQNormalListener> listener3 = std::make_shared<RMQNormalListener>("Listener3");
//     std::shared_ptr<RMQNormalListener> listener4 = std::make_shared<RMQNormalListener>("Listener4");
//     auto pushConsumer1 = ConsumerFactory::getPushConsumer(topic,group,tag,listener1);
//     auto pushConsumer2 = ConsumerFactory::getPushConsumer(topic,group,tag,listener2);
//     auto pushConsumer3 = ConsumerFactory::getPushConsumer(topic,group,tag,listener3);
//     auto pushConsumer4 = ConsumerFactory::getPushConsumer(topic,group,tag,listener4);

//     auto producer = ProducerFactory::getRMQProducer(group);

//     ASSERT_NE(producer, nullptr);

//     for(int i=0;i<SEND_NUM;i++){
//         auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
//         producer->send(message);
//     }

//     long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+240*1000L;
//     while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
//         std::cout<<listener1->getDequeueMessages()->getDataSize() +
//            listener2->getDequeueMessages()->getDataSize() +
//            listener3->getDequeueMessages()->getDataSize() +
//            listener4->getDequeueMessages()->getDataSize() <<std::endl;
//         if(listener1->getDequeueMessages()->getDataSize() +
//            listener2->getDequeueMessages()->getDataSize() +
//            listener3->getDequeueMessages()->getDataSize() +
//            listener4->getDequeueMessages()->getDataSize() == SEND_NUM){
//             break;
//         }
//         std::this_thread::sleep_for(std::chrono::seconds(5));
//     }
//     ASSERT_EQ(SEND_NUM/4,listener1->getDequeueMessages()->getDataSize());
//     ASSERT_EQ(SEND_NUM/4,listener2->getDequeueMessages()->getDataSize());
//     ASSERT_EQ(SEND_NUM/4,listener3->getDequeueMessages()->getDataSize());
//     ASSERT_EQ(SEND_NUM/4,listener4->getDequeueMessages()->getDataSize());
// }
