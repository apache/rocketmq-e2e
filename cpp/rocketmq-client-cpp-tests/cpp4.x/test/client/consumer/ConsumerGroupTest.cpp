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
#include <stdlib.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <rocketmq/MQClientException.h>
#include <rocketmq/DefaultMQPullConsumer.h>
#include <rocketmq/MQMessageQueue.h>
#include "resource/Resource.h"
#include "enums/MessageType.h"
#include "frame/BaseOperate.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

//Use the built-in ConsumerGroup[DEFAULT_CONSUMER] to consume messages and expect consume failed
TEST(ConsumerGroupTest, testSystemInnerConsumerGroup) {
    std::string groupId = "DEFAULT_CONSUMER";
    std::string topic = getTopic(MessageType::NORMAL, "testSystemInnerConsumerGroup", resource->getBrokerAddr(),resource->getNamesrv(),resource->getCluster());
    ASSERT_THROW({
        rocketmq::DefaultMQPullConsumer consumer(groupId);
        consumer.setNamesrvAddr(resource->getNamesrv());
        consumer.registerMessageQueueListener(topic, NULL);
        consumer.start();
        std::vector<rocketmq::MQMessageQueue> mqs;

        try {
            consumer.fetchSubscribeMessageQueues(topic, mqs);
            auto iter = mqs.begin();
            for (; iter != mqs.end(); ++iter) {
                spdlog::info("mq: {}", (*iter).toString());
            }
        } catch (const rocketmq::MQException& e) {
            multi_logger->info("fetchSubscribeMessageQueues exception: {}", e.what());
        }
        consumer.shutdown();
    },rocketmq::MQException);
}