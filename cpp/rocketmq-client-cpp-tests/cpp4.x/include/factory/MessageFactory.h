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

#include "resource/Resource.h"
#include "utils/RandomUtils.h"
#include "utils/NameUtils.h"
#include "spdlog/logger.h"
#include "rocketmq/MQMessage.h"
#include <memory>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class MessageFactory
{
public:
    static rocketmq::MQMessage getRandomMessgae(const std::string &topic);

    static rocketmq::MQMessage getStringMessage(const std::string &topic, std::string &body);

    static rocketmq::MQMessage getStringMessageByTag(const std::string &topic, const std::string &tags, const std::string &body);

    static rocketmq::MQMessage getRandomMessageByTag(const std::string &topic, std::string &tags);

    static rocketmq::MQMessage buildMessage(const std::string &topic);

    static rocketmq::MQMessage buildMessage(const std::string &topic, const std::string &tags);

    static rocketmq::MQMessage buildMessage(const std::string &topic, const std::string &tags, const std::string &body);

    static rocketmq::MQMessage buildMessageOnlyTag(const std::string &topic, const std::string &tags, const std::string &body);

    static rocketmq::MQMessage buildDelayMessage(const std::string &topic, const std::string &tags, const std::string &body, int delayTimeLevel);

    // static rocketmq::MQMessage buildOrderMessage(const std::string& topic, const std::string& tags, const std::string& body, const std::string& messageGroup);

    static rocketmq::MQMessage buildMessageWithProperty(const std::string &topic, std::map<std::string, std::string> &properties);

    static rocketmq::MQMessage buildMessageWithProperty(const std::string &topic, const std::string &messageBody, std::map<std::string, std::string> &properties);

    // static rocketmq::MQMessage buildOrderMessageWithProperty(const std::string& topic, const std::string& messageBody, const std::string& messageGroup, std::map<std::string,std::string>& properties);

    static rocketmq::MQMessage buildMessageWithProperty(const std::string &topic, const std::string &tag, const std::string &body, std::map<std::string, std::string> &properties, const std::vector<std::string> &keys);
};