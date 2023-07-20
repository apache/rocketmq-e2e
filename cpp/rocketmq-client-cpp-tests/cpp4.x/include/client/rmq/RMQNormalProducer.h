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
#include "common/AbstractMQProducer.h"
#include "resource/Resource.h"
#include <memory>
#include <rocketmq/DefaultMQProducer.h>
#include <spdlog/logger.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class RMQNormalProducer : public AbstractMQProducer {
private:
    std::shared_ptr<rocketmq::DefaultMQProducer> producer;
public:
    RMQNormalProducer(std::shared_ptr<rocketmq::DefaultMQProducer> producer){
        this->producer = producer;
    }

    std::shared_ptr<rocketmq::DefaultMQProducer> getProducer(){
        return producer;
    }

    void start() {
        producer->start();
    }

    void send(rocketmq::MQMessage& msg) {
        try {
            rocketmq::SendResult sendResult = producer->send(msg);
            getEnqueueMessages()->addData(msg.getBody());
            // Log sendResult
        } catch (const std::exception& e) {
            multi_logger->error("Producer send message failed, {}", e.what());
        }
    }

    void send(const std::string& topic, const std::string& tags, const std::string& body) {
        rocketmq::MQMessage msg(topic, // topic
                                tags,  // tags
                                body); // body
        send(msg);
    }

    void shutdown() {
        producer->shutdown();
    }
};