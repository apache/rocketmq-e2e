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
#include <vector>
#include <mutex>
#include <thread>
#include "gtest/gtest.h"
#include "rocketmq/DefaultMQPullConsumer.h"
#include "rocketmq/MQMessage.h"
#include "resource/Resource.h"
#include "frame/BaseOperate.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

// Test the normal launch of PullConsumer and expect success
TEST(PullConsumerInitTest, testNormalPullConsumer)
{
    SCOPED_TRACE("Start [PullConsumer] failed, expected success.");
    std::string groupId = getGroupId("testNoClientConfiguration");
    std::string topic = getTopic(MessageType::NORMAL, "testNoClientConfiguration", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    ASSERT_NO_THROW({
        rocketmq::DefaultMQPullConsumer consumer(groupId);
        consumer.setNamesrvAddr(resource->getNamesrv());
        consumer.setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
        consumer.registerMessageQueueListener(topic, NULL);
        consumer.start();
        std::this_thread::sleep_for(std::chrono::seconds(5));
        consumer.shutdown();
    });
}