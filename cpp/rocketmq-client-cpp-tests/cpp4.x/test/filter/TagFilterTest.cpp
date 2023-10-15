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
#include <memory>
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
#include "listener/rmq/RMQNormalListener.h"
#include "resource/Resource.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"
#include "utils/RandomUtils.h"
#include "utils/NameUtils.h"
#include "utils/VerifyUtils.h"
#include "client/rmq/RMQNormalProducer.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

//Using tagA sent 10 messages, the use of tagA | | tagB filter messages, expect consumption to send 10 messages
TEST(TagFilterTest, testSendTagA_SubTagAorTagB){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSendTagA_SubTagAorTagB", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSendTagA_SubTagAorTagB");
    std::string sendTag = NameUtils::getRandomTagName();
    std::string receiveTag = sendTag + "||TagB";
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        multi_logger->info("Wait for the PullConsumer");

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTag,SEND_NUM);

        ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//Use tagA sent 10 messages first, after using tagB sent 10 messages, use tagA | | tagB filter messages, expect consumption to send 20 messages
TEST(TagFilterTest, testSndTagATagB_SubTagATagB){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSndTagATagB_SubTagATagB", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSndTagATagB_SubTagATagB");
    std::string sendTagA = NameUtils::getRandomTagName();
    std::string sendTagB = NameUtils::getRandomTagName();
    std::string receiveTag = sendTagA + "||" + sendTagB;
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        multi_logger->info("Wait for the PullConsumer");

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);
        producer->send(topic,sendTagB,SEND_NUM);

        ASSERT_EQ(SEND_NUM*2,producer->getEnqueueMessages()->getDataSize());

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//The tagA is used to send 10 messages, then the tagB is used to send 10 messages, and the * is used to filter the messages, expecting to consume 20 messages sent
TEST(TagFilterTest, testSendTagAAndTagB_SubAll){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSendTagAAndTagB_SubAll", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSendTagAAndTagB_SubAll");
    std::string sendTagA = NameUtils::getRandomTagName();
    std::string sendTagB = NameUtils::getRandomTagName();
    std::string receiveTag = "*";
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);
        producer->send(topic,sendTagB,SEND_NUM);

        ASSERT_EQ(SEND_NUM*2,producer->getEnqueueMessages()->getDataSize());

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//Send 10 tagA messages, subscribe to tagB messages, expect to consume up to 0 messages
TEST(TagFilterTest, testSendTagA_SubTagB){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSendTagA_SubTagB", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSendTagA_SubTagB");
    std::string sendTag = NameUtils::getRandomTagName();
    std::string receiveTag = NameUtils::getRandomTagName();
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTag,SEND_NUM);

        ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

        std::this_thread::sleep_for(std::chrono::seconds(20));

        ASSERT_EQ(0,pushConsumer->getListener()->getDequeueMessages()->getDataSize());

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//Send 10 tagA messages, subscribe to tagA messages, expect to consume up to 10 messages
TEST(TagFilterTest, testSendTagA_SubTagA){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSendTagA_SubTagA", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSendTagA_SubTagA");
    std::string sendTagA = NameUtils::getRandomTagName();
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,sendTagA,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,sendTagA,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);

        ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//Consumption uses a very long tagA, sending 10 messages, expecting to consume 10 tagA messages
TEST(TagFilterTest, testLongTagSize){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testLongTagSize", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testLongTagSize");
    std::string sendTag = RandomUtils::getStringWithNumber(1024 * 10);
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,sendTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,sendTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTag,SEND_NUM);

        ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//The consumption uses a space-spaced tag, and two tags are used to send 10 messages each, with the expectation of consuming up to 20 messages
TEST(TagFilterTest, testSubTagWithSpace){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSubTagWithSpace", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSubTagWithSpace");
    std::string sendTagA = NameUtils::getRandomTagName();
    std::string sendTagB = NameUtils::getRandomTagName();
    std::string receiveTag = " " + sendTagA + " || " + sendTagB + " ";
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);
        producer->send(topic,sendTagB,SEND_NUM);

        ASSERT_EQ(SEND_NUM*2,producer->getEnqueueMessages()->getDataSize());

        ASSERT_TRUE(VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages())));

        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

////TEST(TagFilterTest, testTagWithSpecialSymbol01){
////    int SEND_NUM = 10;
////    SCOPED_TRACE("Send messages with  tag \"|@\", Expected send() to throw exception, but it didn't");
////    std::string topic = getTopic(MessageType::NORMAL, "testTagWithSpecialSymbol01", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////    std::string group = getGroupId("testTagWithSpecialSymbol01");
////    
////    auto producer = ProducerFactory::getRMQProducer(group);
////
////    ASSERT_NE(producer, nullptr);
////
////    ASSERT_THROW({
////        producer->send(topic,"|@",SEND_NUM);
////    }, std::exception);
////
////    producer->shutdown();
////}

//Send 10 messages with tag='*', subscribe to messages with tag='*', expect to consume the message
TEST(TagFilterTest, testTagWithSpecialSymbol02){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testTagWithSpecialSymbol02", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTagWithSpecialSymbol02");
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,"*",std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,"*",pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,"*",SEND_NUM);

        ASSERT_EQ(SEND_NUM,producer->getEnqueueMessages()->getDataSize());

        VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages()));
        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//Consumer use | | separators between the tag, respectively using two tag each 10 messages sent, and expect consumption to 20 messages
TEST(TagFilterTest, testTagWithSpecialSymbol03){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testTagWithSpecialSymbol03", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTagWithSpecialSymbol03");
    std::string sendTagA = NameUtils::getRandomTagName();
    std::string sendTagB = NameUtils::getRandomTagName();
    std::string receiveTag = sendTagA + "||||" + sendTagB;
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);
        producer->send(topic,sendTagB,SEND_NUM);

        ASSERT_EQ(SEND_NUM*2,producer->getEnqueueMessages()->getDataSize());

        VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages()));
        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

////TEST(TagFilterTest, testTagWithBlankSymbol){
////    int SEND_NUM = 10;
////    std::string topic = getTopic(MessageType::NORMAL, "testTagWithBlankSymbol", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
////    std::string group = getGroupId("testTagWithBlankSymbol");
////    std::string sendTagA = "";
////    std::string sendTagB = " ";
////    ASSERT_NO_THROW({
////        auto producer = ProducerFactory::getRMQProducer(group);
////
////        ASSERT_NE(producer, nullptr);
////
////        ASSERT_THROW({
////            producer->send(topic,sendTagA,SEND_NUM);
////        }, std::exception);
////        
////        ASSERT_THROW({
////            producer->send(topic,sendTagB,SEND_NUM);
////        }, std::exception);
////        producer->shutdown();
////    });
////}

//The sent tag uses two strings with the same hash value, and the consumed tag uses BB, expecting to consume messages with tag=BB
TEST(TagFilterTest, testSendTagWithSameHashCode_SubWithOne){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testSendTagWithSameHashCode_SubWithOne", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testSendTagWithSameHashCode_SubWithOne");
    std::string sendTagA = "BB";
    std::string sendTagB = "Aa";
    std::string receiveTag = "BB";
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);

        VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages()));

        pushConsumer->getListener()->clearMsg();

        producer->send(topic,sendTagB,SEND_NUM);

        std::this_thread::sleep_for(std::chrono::seconds(10));

        ASSERT_EQ(0,pushConsumer->getListener()->getDequeueMessages()->getDataSize());
        
        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}

//Send 10 messages with tag=BB, 10 messages with tag=bb, subscribe with tag=BB, expect case-sensitive messages to be consumed to tag=BB
TEST(TagFilterTest, testTagCaseSensitive){
    int SEND_NUM = 10;
    std::string topic = getTopic(MessageType::NORMAL, "testTagCaseSensitive", resource->getBrokerAddr(), resource->getNamesrv(),resource->getCluster());
    std::string group = getGroupId("testTagCaseSensitive");
    std::string sendTagA = "BB";
    std::string sendTagB = "bb";
    std::string receiveTag = "BB";
    ASSERT_NO_THROW({
        auto pushConsumer = ConsumerFactory::getRMQPushConsumer(topic,group,receiveTag,std::make_shared<RMQNormalListener>());

        auto pullConsumer = ConsumerFactory::getRMQPullConsumer(topic,group);

        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        ASSERT_TRUE(VerifyUtils::tryReceiveOnce(topic,receiveTag,pullConsumer->getPullConsumer()));

        auto producer = ProducerFactory::getRMQProducer(group);

        ASSERT_NE(producer, nullptr);

        producer->send(topic,sendTagA,SEND_NUM);

        VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()),*(pushConsumer->getListener()->getDequeueMessages()));

        pushConsumer->getListener()->clearMsg();

        producer->send(topic,sendTagB,SEND_NUM);

        std::this_thread::sleep_for(std::chrono::seconds(10));

        ASSERT_EQ(0,pushConsumer->getListener()->getDequeueMessages()->getDataSize());
        
        pushConsumer->shutdown();
        pullConsumer->shutdown();
        producer->shutdown();
    });
}