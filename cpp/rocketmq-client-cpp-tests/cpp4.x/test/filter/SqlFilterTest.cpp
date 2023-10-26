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
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/RandomUtils.h"
#include "utils/NameUtils.h"
#include "utils/VerifyUtils.h"
#include "client/rmq/RMQNormalProducer.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

//// TEST(SqlFilterTest, testSendWithTagAndPropsRecvWithOutFilter){
////     int SEND_NUM = 10;
////     std::string topic = getTopic(MessageType::NORMAL, "testSendWithTagAndPropsRecvWithOutFilter", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////     std::string group = getGroupId("testSendWithTagAndPropsRecvWithOutFilter");
////     std::map<std::string, std::string> properties;
////     properties["regionId"] = "cn-hangzhou";
////     properties["price"] = "30";
////     std::string subExpression = "TRUE";
////     ASSERT_NO_THROW({
////         auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,subExpression,std::make_shared<RMQNormalListener>());
////         auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,subExpression,pullConsumer->getPullConsumer()));
////         auto producer = ProducerFactory::getRMQProducer(group);
////         ASSERT_NE(producer, nullptr);
////         for (int i=0; i<SEND_NUM; i++) {
////             rocketmq::MQMessage msg(topic, RandomUtils::getStringByUUID());
////             msg.setProperties(properties);
////             producer->send(msg);
////         }
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());
////         ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));
////         pushConsumer->shutdown();
////         pullConsumer->shutdown();
////         producer->shutdown();
////     });
//// }

//// TEST(SqlFilterTest, testSqlSendTwoProps_SubFilterOne){
////     int SEND_NUM = 10;
////     std::string topic = getTopic(MessageType::NORMAL, "testSqlSendTwoProps_SubFilterOne", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////     std::string group = getGroupId("testSqlSendTwoProps_SubFilterOne");
////     std::map<std::string, std::string> properties1;
////     properties1["price"] = "10";
////     std::map<std::string, std::string> properties2;
////     properties2["price"] = "30";
////     std::string subExpression = "*";
////     ASSERT_NO_THROW({
////         auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,subExpression,std::make_shared<RMQNormalListener>());
////         auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,subExpression,pullConsumer->getPullConsumer()));
////         auto producer = ProducerFactory::getRMQProducer(group);
////         ASSERT_NE(producer, nullptr);
////         for (int i=0; i<SEND_NUM; i++) {
////             rocketmq::MQMessage msg(topic, RandomUtils::getStringByUUID());
////             msg.setProperties(properties1);
////             producer->send(msg);
////         }
////         for (int i=0; i<SEND_NUM; i++) {
////             rocketmq::MQMessage msg(topic, RandomUtils::getStringByUUID());
////             msg.setProperties(properties2);
////             producer->send(msg);
////         }
////         std::this_thread::sleep_for(std::chrono::seconds(5));
////         ASSERT_EQ(SEND_NUM*2,producer->getEnqueueMessages()->getDataSize());
////         ASSERT_TRUE(VerifyUtils::verifyNormalMessageWithUserProperties(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages()),properties1,10));
////         pushConsumer->shutdown();
////         pullConsumer->shutdown();
////         producer->shutdown();
////     });
//// }