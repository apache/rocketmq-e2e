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
#include "utils/data/collect/DataCollector.h"
#include "client/rmq/RMQNormalProducer.h"
#include "common/MQMsg.h"
#include <memory>
#include <rocketmq/DefaultMQPullConsumer.h>
#include <unordered_map>
#include <sstream>

class VerifyUtils {
private:
    const static int TIMEOUT = 90;
    const static int defaultSimpleThreadNums = 4;
    static std::atomic<int> receivedIndex;
    static std::unordered_map<std::string, long> checkDelay(DataCollector<MQMsg>& dequeueMessages, int delayLevel);
    static bool checkOrder(DataCollector<MQMsg>& dequeueMessages);
    static std::vector<rocketmq::MQMessageExt> msgs;
    static std::vector<std::string> waitForMessageConsume(DataCollector<std::string>& enqueueMessages,DataCollector<std::string>& dequeueMessages,long long timeoutMills, int consumedTimes);
    static std::vector<std::string> waitForMessageConsume(DataCollector<std::string>& enqueueMessages,DataCollector<MQMsg>& dequeueMessages,long long timeoutMills, int consumedTimes);
public:
    VerifyUtils() = delete;
    static long long getDelayTime(int delayLevel);
    static bool checkOrderMessage(std::unordered_map<std::string, std::vector<MQMsg>>& receivedMessage);
    static bool tryReceiveOnce(const std::string& topic,const std::string& subExpression, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer);
    static std::vector<rocketmq::MQMessageExt> fetchMessages(std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer, const std::string& topic);
    static bool verifyNormalMessage(DataCollector<std::string>& enqueueMessages, DataCollector<std::string>& dequeueMessages);
    static bool verifyNormalMessage(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages);
    static bool verifyNormalMessageWithUserProperties(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages,std::map<std::string, std::string>& props,int expectedUnrecvMsgNum);
    static bool verifyDelayMessage(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages,int delayLevel);
    static bool verifyOrderMessage(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages);
    static bool waitReceiveThenAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum);
    static bool waitFIFOParamReceiveThenNAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum);
    static bool waitFIFOParamReceiveThenAckExceptedLast(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum=3);
    static bool waitFIFOReceiveThenAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum);
    static bool waitAckExceptionReReceiveAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum);
    static bool waitReceiveMaxsizeSync(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum);
};