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
#include "common/AbstractMQConsumer.h"
#include "listener/MsgListener.h"
#include "resource/Resource.h"
#include "common/MQCollector.h"
#include "listener/rmq/RMQNormalListener.h"
#include <future>
#include <mutex>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <vector>
#include <memory>
#include <string>
#include <optional>
#include <chrono>
#include <rocketmq/DefaultMQPullConsumer.h>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <spdlog/logger.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class RMQNormalConsumer : public AbstractMQConsumer {
private:
    std::shared_ptr<rocketmq::DefaultMQPushConsumer> pushConsumer;
    std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer;
    std::shared_ptr<rocketmq::DefaultMQPushConsumer> consumer;
    std::vector<std::future<void>> executorService;
    static constexpr int WAIT_RESPONSE_MILLS = 15 * 1000;
    std::shared_ptr<RMQNormalListener> listener = nullptr;
    bool needRun = true;
    int consumerThreadNum = 20;
    static std::atomic<int> receivedIndex;

public:
    RMQNormalConsumer(std::shared_ptr<rocketmq::DefaultMQPushConsumer> consumer, std::shared_ptr<RMQNormalListener> listener) 
        : pushConsumer(consumer), listener(listener) {}

    RMQNormalConsumer(std::shared_ptr<rocketmq::DefaultMQPullConsumer> consumer) 
        : pullConsumer(consumer) {}

    // void receiveThenNack(const std::string& topic,int maxMessageNum, std::optional<std::chrono::duration<double>> receiveInvisibleDuration, std::optional<std::chrono::duration<double>> changeInvisibleDuration) { 
        
    // }

    void shutdown() {
        if (pushConsumer) {
            pushConsumer->shutdown();
        }
        if (pullConsumer) {
            pullConsumer->shutdown();
        }
    }

    std::shared_ptr<rocketmq::DefaultMQPushConsumer> getPushConsumer() {
        return pushConsumer;
    }

    void setPushConsumer(std::shared_ptr<rocketmq::DefaultMQPushConsumer> pushConsumer) {
        this->pushConsumer = pushConsumer;
    }

    std::shared_ptr<rocketmq::DefaultMQPullConsumer> getPullConsumer() {
        return pullConsumer;
    }

    void setSimpleConsumer(std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer) {
        this->pullConsumer = pullConsumer;
    }

    std::shared_ptr<rocketmq::DefaultMQPushConsumer> getConsumer() {
        return consumer;
    }

    void setConsumer(std::shared_ptr<rocketmq::DefaultMQPushConsumer> consumer) {
        this->consumer = consumer;
    }

    // std::shared_ptr<RMQNormalListener> getListener() {
    //     return listener;
    // }

    // void setListener(std::shared_ptr<RMQNormalListener> listener) {
    //     this->listener = listener;
    // }
};