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
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <mutex>
#include <chrono>
#include <thread>
#include <spdlog/spdlog.h>
#include <rocketmq/MQMessage.h>
#include <rocketmq/DefaultMQProducer.h>
#include <rocketmq/DefaultMQPushConsumer.h>
#include <rocketmq/DefaultMQPullConsumer.h>
#include "frame/BaseOperate.h"
#include "listener/MsgListener.h"
#include "resource/Resource.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/VerifyUtils.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

TEST(MessageBodyContentTest, testMessageBodyContentIsSpace){
    std::string topic = "TEST";//getTopic(MessageType::NORMAL, "testMessageBodyContentIsSpace", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = "group";//getGroupId("testMessageBodyContentIsSpace");
    ASSERT_NO_FATAL_FAILURE({
        MsgListener msglistener;
        auto pushConsumer = PushConsumerFactory::getPushConsumer(topic,group,msglistener);
        pushConsumer->shutdown();

        auto pullConsumer = PullConsumerFactory::getPullConsumer(topic,group);
        
        VerifyUtils::tryReceiveOnce(topic,pullConsumer);

        auto producer = ProducerFactory::getProducer(group);

        std::string body = " ";
        rocketmq::MQMessage msg(topic,"*",body);
        rocketmq::SendResult sendResult = producer->send(msg);

        ASSERT_EQ(sendResult.getSendStatus(), rocketmq::SendStatus::SEND_OK);

        std::vector<rocketmq::MQMessageQueue> mqs;
        pullConsumer->fetchSubscribeMessageQueues(topic, mqs);

        pullConsumer->shutdown();
        producer->shutdown();
    });
}