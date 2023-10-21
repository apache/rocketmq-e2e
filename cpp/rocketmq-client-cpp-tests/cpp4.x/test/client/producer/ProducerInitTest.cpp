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
#include <thread>
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "rocketmq/DefaultMQProducer.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/RandomUtils.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

////TEST(ProducerInitTest, testErrorNameSrvAddr){
////    std::string groupId = getGroupId("testErrorNameSrvAddr");
////    ASSERT_ANY_THROW({
////        rocketmq::DefaultMQProducer producer(groupId);
////        producer.setTcpTransportTryLockTimeout(1000);
////        producer.setTcpTransportConnectTimeout(400);
////        producer.setNamesrvDomain("https://www.aliyun.com");
////        producer.start();
////        std::this_thread::sleep_for(std::chrono::seconds(5));
////        producer.shutdown();
////    });
////}