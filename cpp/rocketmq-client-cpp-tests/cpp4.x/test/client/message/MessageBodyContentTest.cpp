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
#include <iostream>
#include <string>
#include <mutex>
#include <chrono>
#include <thread>
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "rocketmq/MQMessage.h"
#include "rocketmq/DefaultMQProducer.h"
#include "rocketmq/DefaultMQPushConsumer.h"
#include "rocketmq/DefaultMQPullConsumer.h"
#include "frame/BaseOperate.h"
#include "listener/MsgListener.h"
#include "resource/Resource.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/VerifyUtils.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

// Send normal message, setting message body with space character, expect consume success
TEST(MessageBodyContentTest, testMessageBodyContentIsSpace)
{
    std::string topic = getTopic(MessageType::NORMAL, "testMessageBodyContentIsSpace", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group = getGroupId("testMessageBodyContentIsSpace");
    ASSERT_NO_THROW({
        std::shared_ptr<MsgListener> msglistener = std::make_shared<MsgListener>();
        auto pushConsumer = ConsumerFactory::getPushConsumer(topic, group, "*", msglistener);
        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto producer = ProducerFactory::getProducer(group);

        std::string body = " ";
        rocketmq::MQMessage msg(topic, "*", body);
        rocketmq::SendResult sendResult = producer->send(msg);

        ASSERT_EQ(sendResult.getSendStatus(), rocketmq::SendStatus::SEND_OK);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        // std::vector<rocketmq::MQMessageExt> msgs = VerifyUtils::fetchMessages(pullConsumer, topic);
        auto msgs = msglistener->getMessages();

        ASSERT_EQ(msgs.size(), 1);
        ASSERT_EQ(msgs[0].getBody(), body);

        pushConsumer->shutdown();
        producer->shutdown();
    });
}

// Send normal message, setting message body with chinese character, expect consume success
TEST(MessageBodyContentTest, testMessageBodyContentIsChinese)
{
    std::string topic = getTopic(MessageType::NORMAL, "testMessageBodyContentIsChinese", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group = getGroupId("testMessageBodyContentIsChinese");
    ASSERT_NO_THROW({
        std::shared_ptr<MsgListener> msglistener = std::make_shared<MsgListener>();
        auto pushConsumer = ConsumerFactory::getPushConsumer(topic, group, "*", msglistener);

        auto producer = ProducerFactory::getProducer(group);

        std::string body = "中文字符";
        rocketmq::MQMessage msg(topic, "*", body);
        rocketmq::SendResult sendResult = producer->send(msg);

        ASSERT_EQ(sendResult.getSendStatus(), rocketmq::SendStatus::SEND_OK);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto msgs = msglistener->getMessages();

        ASSERT_EQ(msgs.size(), 1);
        ASSERT_EQ(msgs[0].getBody(), body);

        pushConsumer->shutdown();
        producer->shutdown();
    });
}

// Send normal message, setting message body with emoji(😱) character, expect consume success
TEST(MessageBodyContentTest, testMessageBodyContentIsEmoji)
{
    std::string topic = getTopic(MessageType::NORMAL, "testMessageBodyContentIsEmoji", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group = getGroupId("testMessageBodyContentIsEmoji");
    ASSERT_NO_THROW({
        std::shared_ptr<MsgListener> msglistener = std::make_shared<MsgListener>();
        auto pushConsumer = ConsumerFactory::getPushConsumer(topic, group, "*", msglistener);
        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto producer = ProducerFactory::getProducer(group);

        std::string body = "😱";
        rocketmq::MQMessage msg(topic, "*", body);
        rocketmq::SendResult sendResult = producer->send(msg);

        ASSERT_EQ(sendResult.getSendStatus(), rocketmq::SendStatus::SEND_OK);

        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto msgs = msglistener->getMessages();

        ASSERT_EQ(msgs.size(), 1);
        ASSERT_EQ(msgs[0].getBody(), body);

        pushConsumer->shutdown();
        producer->shutdown();
    });
}