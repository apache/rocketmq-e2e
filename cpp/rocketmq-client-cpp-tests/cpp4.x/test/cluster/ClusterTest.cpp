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
#include <memory>
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
#include "listener/rmq/RMQNormalListener.h"
#include "resource/Resource.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "client/rmq/RMQNormalProducer.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

// Send 100 normal messages synchronously, start two consumers on different GroupId, and expect each client to consume up to 100 messages
TEST(ClusterTest, testBroadcastConsume)
{
    std::string topic = getTopic(MessageType::NORMAL, "testBroadcastConsume", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group1 = getGroupId("testBroadcastConsume1");
    std::string group2 = getGroupId("testBroadcastConsume2");
    std::string group = getGroupId("testBroadcastConsume");

    std::shared_ptr<RMQNormalListener> listener1 = std::make_shared<RMQNormalListener>("Listener1");
    std::shared_ptr<RMQNormalListener> listener2 = std::make_shared<RMQNormalListener>("Listener2");
    auto pushConsumer1 = ConsumerFactory::getBroadcastPushConsumer(topic, group1, "*", listener1);
    auto pushConsumer2 = ConsumerFactory::getBroadcastPushConsumer(topic, group2, "*", listener2);

    auto producer = ProducerFactory::getRMQProducer(group);

    for (int i = 0; i < 100; i++)
    {
        rocketmq::MQMessage msg(topic, "*", RandomUtils::getStringByUUID());
        rocketmq::SendResult sendResult = producer->send(msg);
        ASSERT_EQ(sendResult.getSendStatus(), rocketmq::SendStatus::SEND_OK);
    }

    ASSERT_EQ(100, producer->getEnqueueMessages()->getDataSize());

    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() + 240 * 1000L;
    while (endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count())
    {
        if (listener1->getDequeueMessages()->getDataSize() == 100 && listener2->getDequeueMessages()->getDataSize() == 100)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()), *(listener1->getDequeueMessages())));
    ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()), *(listener2->getDequeueMessages())));

    pushConsumer1->shutdown();
    pushConsumer2->shutdown();
    producer->shutdown();
}

// Send 100 normal messages synchronously, start three consumers on different GroupId, and expect each client to consume up to 100 messages
TEST(ClusterTest, testClusterConsume)
{
    std::string topic = getTopic(MessageType::NORMAL, "testClusterConsume", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group1 = getGroupId("testClusterConsume1");
    std::string group2 = getGroupId("testClusterConsume2");
    std::string group3 = getGroupId("testClusterConsume3");
    ASSERT_NO_THROW({
        std::shared_ptr<RMQNormalListener> listener1 = std::make_shared<RMQNormalListener>("Listener1");
        std::shared_ptr<RMQNormalListener> listener2 = std::make_shared<RMQNormalListener>("Listener2");
        std::shared_ptr<RMQNormalListener> listener3 = std::make_shared<RMQNormalListener>("Listener3");
        auto pushConsumer1 = ConsumerFactory::getPushConsumer(topic, group1, "*", listener1);
        auto pushConsumer2 = ConsumerFactory::getPushConsumer(topic, group2, "*", listener2);
        auto pushConsumer3 = ConsumerFactory::getPushConsumer(topic, group3, "*", listener3);

        auto producer = ProducerFactory::getRMQProducer(group1);

        int count = 0;
        for (int i = 0; i < 100; i++)
        {
            rocketmq::MQMessage msg(topic, "*", RandomUtils::getStringByUUID());
            rocketmq::SendResult sendResult = producer->send(msg);
            if (sendResult.getSendStatus() == rocketmq::SendStatus::SEND_OK)
            {
                count++;
            }
        }

        ASSERT_EQ(count, 100);

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()), *(listener1->getDequeueMessages())));
        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()), *(listener2->getDequeueMessages())));
        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()), *(listener3->getDequeueMessages())));

        pushConsumer1->shutdown();
        pushConsumer2->shutdown();
        pushConsumer3->shutdown();
        producer->shutdown();
    });
}