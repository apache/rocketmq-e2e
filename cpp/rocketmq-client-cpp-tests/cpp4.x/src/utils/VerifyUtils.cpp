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

bool async_function(std::string topic, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer){
    std::vector<rocketmq::MQMessageQueue> mqs;
    try {
        pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
        for (auto& mq : mqs) {
            long long offset = pullConsumer->fetchConsumeOffset(mq, true);
            if(offset<0) continue;
            rocketmq::PullResult pullResult = pullConsumer->pull(mq, "", offset, 32);
            switch (pullResult.pullStatus) {
                case rocketmq::FOUND:
                    for (auto& msg : pullResult.msgFoundList) {
                        multi_logger->info("Message: {}", msg.toString());
                    }
                    offset = pullResult.nextBeginOffset;
                    pullConsumer->updateConsumeOffset(mq, offset);
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
        }
    } catch (const rocketmq::MQException& e) {
        multi_logger->warn("fetchSubscribeMessageQueues exception: {}", e.what());
        return false;
    }
    return true;
}

bool VerifyUtils::tryReceiveOnce(std::string topic, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer){
    // async_function(topic, pullConsumer);
    std::future<bool> future1 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future2 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future3 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future4 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future5 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    
    auto status1 = future1.wait_for(std::chrono::seconds(30));
    // auto status2 = future2.wait_for(std::chrono::seconds(30));
    // auto status3 = future3.wait_for(std::chrono::seconds(30));
    // auto status4 = future4.wait_for(std::chrono::seconds(30));
    // auto status5 = future5.wait_for(std::chrono::seconds(30));

    if (status1 == std::future_status::ready && future1.get() == true) {
        return true;
    } else {
        return false;
    }
}

std::vector<rocketmq::MQMessageExt> VerifyUtils::fetchMessages(std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer, const std::string& topic) {
        std::vector<rocketmq::MQMessageQueue> mqs;
        pullConsumer->fetchSubscribeMessageQueues(topic, mqs);

        //rocekmq获取队列中所有未消费的消息，首先判断消息数量是不是为1，然后判断消息体是否为空
        std::vector<rocketmq::MQMessageExt> msgs;
        for (auto& mq : mqs) {
            long long offset = pullConsumer->fetchConsumeOffset(mq, true);
            if(offset<0) continue;
            rocketmq::PullResult pullResult = pullConsumer->pull(mq, "", offset, 32);
            switch (pullResult.pullStatus) {
                case rocketmq::FOUND:
                    for (auto& msg : pullResult.msgFoundList) {
                        msgs.push_back(msg);
                        // std::cout << "msg body: " << msg.getBody() << std::endl;
                    }
                    offset = pullResult.nextBeginOffset;
                    pullConsumer->updateConsumeOffset(mq, offset);
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
        }
        return msgs;
    }