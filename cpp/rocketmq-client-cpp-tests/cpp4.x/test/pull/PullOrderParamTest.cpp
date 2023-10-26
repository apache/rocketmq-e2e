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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <cassert>
#include <string>
#include "gtest/gtest.h"
#include "rocketmq/SendResult.h"
#include "spdlog/logger.h"
#include "enums/MessageType.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/NameUtils.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "factory/MessageFactory.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

// When sending 20 sequential messages synchronously using the same queue, PullConsumer normally receives messages, but does not ack messages, and keeps the sequence; the messages are stuck at the first
TEST(PullOrderParamTest, testFIFO_pull_receive_nack)
{
    int SEND_NUM = 20;
    std::string topic = getTopic(MessageType::FIFO, "testFIFO_pull_receive_nack", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group = getGroupId("testFIFO_pull_receive_nack");
    std::string tag = NameUtils::getRandomTagName();

    auto pullConsumer = ConsumerFactory::getPullConsumer(topic, group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic, tag, pullConsumer));

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);

    for (int i = 0; i < SEND_NUM; i++)
    {
        auto message = MessageFactory::buildMessage(topic, tag, std::to_string(i));
        producer->sendOrderMessage(message, 0);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_EQ(SEND_NUM, producer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::waitFIFOParamReceiveThenNAck(producer, pullConsumer, topic, tag, 1));

    pullConsumer->shutdown();
    producer->shutdown();
}

// Twenty sequential messages are sent synchronously and receive 3 messages in batch. All pulled messages are ack messages except the last one. It is expected that all messages remain sequential and are consumed again after a certain time
TEST(PullOrderParamTest, testFIFO_pull_receive_multi_nack)
{
    int SEND_NUM = 20;
    std::string topic = getTopic(MessageType::FIFO, "testFIFO_pull_receive_multi_nack", resource->getBrokerAddr(), resource->getNamesrv(), resource->getCluster());
    std::string group = getGroupId("testFIFO_pull_receive_multi_nack");
    std::string tag = NameUtils::getRandomTagName();

    auto pullConsumer = ConsumerFactory::getPullConsumer(topic, group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic, tag, pullConsumer));

    auto producer = ProducerFactory::getRMQProducer(group);

    ASSERT_NE(producer, nullptr);

    for (int i = 0; i < SEND_NUM; i++)
    {
        auto message = MessageFactory::buildMessage(topic, tag, std::to_string(i));
        producer->sendOrderMessage(message, 0);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_EQ(SEND_NUM, producer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::waitFIFOParamReceiveThenAckExceptedLast(producer, pullConsumer, topic, tag, 20));

    pullConsumer->shutdown();
    producer->shutdown();
}