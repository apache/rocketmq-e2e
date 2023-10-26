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
#include <string>
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "rocketmq/MQMessage.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/RandomUtils.h"
#include "factory/ProducerFactory.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

////TEST(MessageUserPropertyTest, testMessageUserPropertyBeyondSize128){
////     SCOPED_TRACE("message user property beyond limit 128 ,expect throw exception but it didn't");
////     std::string topic = getTopic(MessageType::NORMAL, "testMessageUserPropertyBeyondSize128", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////     std::string group = getGroupId("testMessageUserPropertyBeyondSize128");
////     auto producer = ProducerFactory::getProducer(group);
////     producer->start();
////     ASSERT_THROW({
////        std::string body = RandomUtils::randomAlphabetic(64);
////        rocketmq::MQMessage msg(topic,body);
////        for (int i = 0; i<=128; i++) {
////            msg.setProperty(std::to_string(i),RandomUtils::randomAlphabetic(2));
////        }
////        producer->send(msg);
////        producer->shutdown();
////     }, std::exception);
////}