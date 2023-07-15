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
#include "utils/VerifyUtils.h"
#include <gtest/gtest.h>
#include <memory>
#include <resource/Resource.h>
#include <spdlog/logger.h>
#include <vector>
#include <future>
#include <chrono>
#include <thread>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

int async_function(std::string topic, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer){
    std::vector<rocketmq::MQMessageQueue> mqs;
    try {
        pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
        auto iter = mqs.begin();
        for (; iter != mqs.end(); ++iter) {
            // multi_logger->info("mq: {}", (*iter).toString());
        }
    } catch (const rocketmq::MQException& e) {
        multi_logger->info("fetchSubscribeMessageQueues exception: {}", e.what());
    }
    return 1;
}

void VerifyUtils::tryReceiveOnce(std::string topic, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer){
    std::future<int> future1 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    auto status1 = future1.wait_for(std::chrono::seconds(30));
    std::future<int> future2 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    auto status2 = future2.wait_for(std::chrono::seconds(30));
    std::future<int> future3 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    auto status3 = future3.wait_for(std::chrono::seconds(30));
    std::future<int> future4 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    auto status4 = future4.wait_for(std::chrono::seconds(30));
    std::future<int> future5 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    auto status5 = future5.wait_for(std::chrono::seconds(30));
    std::this_thread::sleep_for(std::chrono::seconds(30));

    if (status1 == std::future_status::ready && status2 == std::future_status::ready && status3 == std::future_status::ready && status4 == std::future_status::ready && status5 == std::future_status::ready) {
    } else {
        FAIL() << "tryReceiveOnce error";
    }
}