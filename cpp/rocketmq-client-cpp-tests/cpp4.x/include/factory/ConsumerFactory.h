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
#include "listener/MsgListener.h"
#include "resource/Resource.h"
#include <memory>
#include <rocketmq/DefaultMQPullConsumer.h>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <spdlog/logger.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class PushConsumerFactory {
public:
    PushConsumerFactory()=delete;

    static std::shared_ptr<rocketmq::DefaultMQPushConsumer> getPushConsumer(std::string topic, std::string group,MsgListener& msglistener){
        auto rmqPushConsumer = std::make_shared<rocketmq::DefaultMQPushConsumer>(group);
        rmqPushConsumer->setNamesrvAddr(resource->getNamesrv());
        rmqPushConsumer->setInstanceName(group);
        rmqPushConsumer->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
        rmqPushConsumer->setConsumeFromWhere(rocketmq::CONSUME_FROM_FIRST_OFFSET);
        rmqPushConsumer->subscribe(topic, "*");
        rmqPushConsumer->registerMessageListener(&msglistener);
        rmqPushConsumer->setConsumeThreadCount(4);
        rmqPushConsumer->start();
        return rmqPushConsumer;
    }
};

class PullConsumerFactory{
public:
    PullConsumerFactory()=delete;

    static std::shared_ptr<rocketmq::DefaultMQPullConsumer> getPullConsumer(std::string topic,std::string group){
        auto rmqPullConsumer = std::make_shared<rocketmq::DefaultMQPullConsumer>(group);
        rmqPullConsumer->setNamesrvAddr(resource->getNamesrv());
        rmqPullConsumer->setInstanceName(group);
        rmqPullConsumer->setSessionCredentials(resource->getAccessKey(), resource->getSecretKey(), resource->getAccessChannel());
        rmqPullConsumer->registerMessageQueueListener(topic, NULL);
        rmqPullConsumer->start();
        return rmqPullConsumer;
    }
};