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

#include <gtest/gtest.h>
#include <rocketmq/SendResult.h>
#include <spdlog/logger.h>
#include <string>

#include "enums/MessageType.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/NameUtils.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "factory/MessageFactory.h"
#include "common/MQTransactionListener.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

//Send 10 transaction messages and synchronously commit the transaction (Checker performs rollback), expecting those 10 messages to be consumed via PushConsumer
TEST(TransactionMessageTest, testTrans_SendCommit_PushConsume){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::TRANSACTION, "testTrans_SendCommit_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTrans_SendCommit_PushConsume");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));

    rocketmq::TransactionListener* listener = new CommitMQTransactionListener();
    auto transProducer = ProducerFactory::getRMQTransProducer(group,listener);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_NE(transProducer, nullptr);

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,RandomUtils::getStringByUUID());
        rocketmq::SendResult sendResult = transProducer->sendTrans(message,rocketmq::LocalTransactionState::COMMIT_MESSAGE);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SendStatus::SEND_OK);
    }

    ASSERT_EQ(SEND_NUM,transProducer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(transProducer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

    pushConsumer->shutdown();
    pullConsumer->shutdown();
    transProducer->shutdownTransaction();
}

//Send 10 transaction messages and rollback directly (Checker does commit), expecting that these 10 messages cannot be consumed by PushConsumer
TEST(TransactionMessageTest, testTrans_SendRollback_PushConsume){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::TRANSACTION, "testTrans_SendRollback_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTrans_SendRollback_PushConsume");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    rocketmq::TransactionListener* listener = new CommitMQTransactionListener();
    auto transProducer = ProducerFactory::getRMQTransProducer(group,listener);

    ASSERT_NE(transProducer, nullptr);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,RandomUtils::getStringByUUID());
        rocketmq::SendResult sendResult = transProducer->sendTrans(message,rocketmq::LocalTransactionState::ROLLBACK_MESSAGE);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SendStatus::SEND_OK);
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(30));

    ASSERT_EQ(SEND_NUM,transProducer->getEnqueueMessages()->getDataSize());

    ASSERT_EQ(0,pushConsumer->getListener()->getDequeueMessages()->getDataSize());

    pushConsumer->shutdown();
    transProducer->shutdownTransaction();
}

//Send 10 transaction messages and COMMIT the transaction by Checker (perform COMMIT), expecting the 10 messages to be consumed by PushConsumer
TEST(TransactionMessageTest, testTrans_SendCheckerCommit_PushConsume){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::TRANSACTION, "testTrans_SendCheckerCommit_PushConsume", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTrans_SendCheckerCommit_PushConsume");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    rocketmq::TransactionListener* listener = new CommitMQTransactionListener();
    auto transProducer = ProducerFactory::getRMQTransProducer(group,listener);

    ASSERT_NE(transProducer, nullptr);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,RandomUtils::getStringByUUID());
        rocketmq::SendResult sendResult = transProducer->sendTrans(message,rocketmq::LocalTransactionState::UNKNOWN);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SendStatus::SEND_OK);
    }

    std::this_thread::sleep_for(std::chrono::seconds(30));

    ASSERT_EQ(SEND_NUM,transProducer->getEnqueueMessages()->getDataSize());

    ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(transProducer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

    pushConsumer->shutdown();
    transProducer->shutdownTransaction();
}

//Send 10 transaction messages and roll back the transaction by Checker (performing ROLLBACK), expecting that the 10 messages will not be consumed by PushConsumer
TEST(TransactionMessageTest, testTrans_CheckerRollback){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::TRANSACTION, "testTrans_CheckerRollback", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTrans_CheckerRollback");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    rocketmq::TransactionListener* listener = new RollbackMQTransactionListener();
    auto transProducer = ProducerFactory::getRMQTransProducer(group,listener);

    ASSERT_NE(transProducer, nullptr);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,RandomUtils::getStringByUUID());
        rocketmq::SendResult sendResult = transProducer->sendTrans(message,rocketmq::LocalTransactionState::UNKNOWN);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SendStatus::SEND_OK);
    }

    std::this_thread::sleep_for(std::chrono::seconds(30));

    ASSERT_EQ(SEND_NUM,transProducer->getEnqueueMessages()->getDataSize());

    ASSERT_EQ(0,pushConsumer->getListener()->getDequeueMessages()->getDataSize());

    pushConsumer->shutdown();
    transProducer->shutdownTransaction();
}

TEST(TransactionMessageTest, testTrans_SendCheckerPartionCommit){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::TRANSACTION, "testTrans_SendCheckerPartionCommit", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTrans_SendCheckerPartionCommit");
    std::string tag = NameUtils::getRandomTagName();

    auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,tag,std::make_shared<RMQNormalListener>());

    auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,tag,pullConsumer->getPullConsumer()));
    pullConsumer->shutdown();

    std::atomic<int> commitMsgNum(0);
    std::atomic<int> rollbackMsgNum(0);
    rocketmq::TransactionListener* listener = new UserdefinedMQTransactionListener(commitMsgNum,rollbackMsgNum);
    auto transProducer = ProducerFactory::getRMQTransProducer(group,listener);

    ASSERT_NE(transProducer, nullptr);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    for(int i=0;i<SEND_NUM;i++){
        auto message = MessageFactory::buildMessage(topic,tag,std::to_string(i));
        rocketmq::SendResult sendResult = transProducer->sendTrans(message,rocketmq::LocalTransactionState::UNKNOWN);
        ASSERT_EQ(sendResult.getSendStatus(),rocketmq::SendStatus::SEND_OK);
    }

    int timeoutSeconds = 90;
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(timeoutSeconds);
    while (commitMsgNum.load() != rollbackMsgNum.load() || commitMsgNum.load() != SEND_NUM/2) {
        if (std::chrono::steady_clock::now() >= endTime) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::this_thread::sleep_for(std::chrono::seconds(30));

    ASSERT_EQ(SEND_NUM,transProducer->getEnqueueMessages()->getDataSize());

    ASSERT_EQ(SEND_NUM/2,pushConsumer->getListener()->getDequeueMessages()->getDataSize());

    pushConsumer->shutdown();
    transProducer->shutdownTransaction();
}