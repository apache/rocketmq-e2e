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
#include "enums/MessageType.h"
#include "frame/BaseOperate.h"
#include "rocketmq/MQClientException.h"
#include "rocketmq/DefaultMQPullConsumer.h"
#include "rocketmq/MQMessageQueue.h"

extern std::string namesrv;
extern std::string brokerAddr;
extern std::string cluster;

TEST(ConsumerGroupTest, testSystemInnerConsumerGroup) {
    std::string groupId = "DEFAULT_CONSUMER";
    std::string topic = getTopic(MessageType::NORMAL, "testSystemInnerConsumerGroup", brokerAddr,namesrv,cluster);
    ASSERT_THROW({
        rocketmq::DefaultMQPullConsumer consumer(groupId);
        consumer.setNamesrvAddr(namesrv);
        consumer.setGroupName(groupId);
        consumer.setInstanceName(groupId);
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
            spdlog::info("fetchSubscribeMessageQueues exception: {}", e.what());
        }
        consumer.shutdown();
    },rocketmq::MQException);
}