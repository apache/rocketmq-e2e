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

#include "common/MQCollector.h"
#include "listener/MsgListener.h"
#include "resource/Resource.h"
#include "utils/RandomUtils.h"
#include <memory>
#include <spdlog/logger.h>
#include <atomic>
#include <iostream>
#include <rocketmq/MQMessageListener.h>
#include <rocketmq/MQMessageExt.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class RMQNormalListener : public MQCollector, public rocketmq::MessageListenerConcurrently {
private:
    rocketmq::ConsumeStatus consumeStatus = rocketmq::CONSUME_SUCCESS;
    std::atomic<int> msgIndex{0};
    std::string listenerName;
    int reconsumeTimes = 0;
    bool isEvenNumber = false;
    int workTime = 0;

public:
    RMQNormalListener() {
        this->listenerName = RandomUtils::getStringByUUID();
        multi_logger->info("start listener: {}",listenerName);
    }

    RMQNormalListener(int reconsumeTimes, rocketmq::ConsumeStatus consumeStatus){
        this->reconsumeTimes = reconsumeTimes;
        this->consumeStatus = consumeStatus;
        this->listenerName = RandomUtils::getStringByUUID();
        multi_logger->info("start listener: {}",listenerName);
    }

    RMQNormalListener(int reconsumeTimes, int workTime){
        this->reconsumeTimes = reconsumeTimes;
        this->workTime = workTime;
        this->listenerName = RandomUtils::getStringByUUID();
        multi_logger->info("start listener: {}",listenerName);
    }

    RMQNormalListener(std::string listenerName) {
        this->listenerName = listenerName;
        multi_logger->info("start listener: {}",listenerName);
    }

    RMQNormalListener(rocketmq::ConsumeStatus consumeStatus){
        this->consumeStatus = consumeStatus;
        this->listenerName = RandomUtils::getStringByUUID();
        multi_logger->info("start listener: {}",listenerName);
    }

    // Overriding the consume method
    rocketmq::ConsumeStatus consume(rocketmq::MQMessageExt& message) override {
        rocketmq::ConsumeStatus result = consumeStatus;
        
        if (reconsumeTimes == 0 || reconsumeTimes == message.getReconsumeTimes()-1) {
            dequeueMessages->addData(message.getBody());
            result = consumeStatus;
        }else{
            // if (isEvenNumber) {
            //     int body = atoi(message.getBody().c_str());
            //     if (body % 2 != 0) {
            //         dequeueMessages->addData(message.getBody());
            //         result = rocketmq::CONSUME_SUCCESS;
            //     }
               
            // }
            std::this_thread::sleep_for(std::chrono::milliseconds(workTime));
        }

        std::string consumeStatStr;
        if(consumeStatus == rocketmq::RECONSUME_LATER) {
            consumeStatStr = "RECONSUME_LATER";
        } else {
            consumeStatStr = "CONSUME_SUCCESS";
        }
        multi_logger->info("{} - MessageId:{}, body:{}, tag:{},  key:{}, recvIndex:{}, property:{}, action:{}",listenerName,message.getMsgId(),message.getBody(),message.getTags(),message.getKeys(),msgIndex.load(),message.getProperties(),consumeStatStr);
        return result;
    }
};