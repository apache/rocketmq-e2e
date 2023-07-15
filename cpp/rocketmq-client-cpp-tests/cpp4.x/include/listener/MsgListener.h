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
#pragma once

#include "resource/Resource.h"
#include <atomic>
#include <memory>
#include <spdlog/logger.h>
#include <vector>
#include <rocketmq/MQMessageExt.h>
#include <rocketmq/MQMessageListener.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class MsgListener : public rocketmq::MessageListenerConcurrently {
private:
    std::atomic<int> count;
    std::vector<std::string> messages;
public:
    MsgListener() { count = 0; };

    void resetMsgCount() { count = 0; }

    int getMsgCount() { return count; }

    virtual ~MsgListener() {}

    virtual rocketmq::ConsumeStatus consumeMessage(const std::vector<rocketmq::MQMessageExt>& msgs) override {
        for (const auto& msg : msgs) {
                multi_logger->info("Received message: {}",msg.toString());
                messages.push_back(msg.toString());
                count++;
        }
        return rocketmq::CONSUME_SUCCESS;
    }
};