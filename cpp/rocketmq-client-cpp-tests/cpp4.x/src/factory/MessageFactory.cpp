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
#include "factory/MessageFactory.h"
#include <string>

rocketmq::MQMessage MessageFactory::getRandomMessgae(const std::string &topic)
{
    std::string body = RandomUtils::getStringByUUID();
    return getStringMessage(topic, body);
}

rocketmq::MQMessage MessageFactory::getStringMessage(const std::string &topic, std::string &body)
{
    rocketmq::MQMessage msg(topic, body);
    return msg;
}

rocketmq::MQMessage MessageFactory::getStringMessageByTag(const std::string &topic, const std::string &tags, const std::string &body)
{
    rocketmq::MQMessage msg(topic, tags, body);
    return msg;
}

rocketmq::MQMessage MessageFactory::getRandomMessageByTag(const std::string &topic, std::string &tags)
{
    std::string body = RandomUtils::getStringByUUID();
    return getStringMessageByTag(topic, tags, body);
}

rocketmq::MQMessage MessageFactory::buildMessage(const std::string &topic)
{
    std::string body = RandomUtils::getStringByUUID();
    std::string tag = NameUtils::getRandomTagName();
    rocketmq::MQMessage msg(topic, tag, body);
    return msg;
}

rocketmq::MQMessage MessageFactory::buildMessage(const std::string &topic, const std::string &tags)
{
    std::string body = RandomUtils::getStringByUUID();
    std::string keys = RandomUtils::getStringByUUID();
    rocketmq::MQMessage msg(topic, tags, keys, body);
    return msg;
}

rocketmq::MQMessage MessageFactory::buildMessage(const std::string &topic, const std::string &tags, const std::string &body)
{
    std::string keys = RandomUtils::getStringByUUID();
    rocketmq::MQMessage msg(topic, tags, keys, body);
    return msg;
}

rocketmq::MQMessage MessageFactory::buildMessageOnlyTag(const std::string &topic, const std::string &tags, const std::string &body)
{
    rocketmq::MQMessage msg(topic, tags, body);
    return msg;
}

rocketmq::MQMessage MessageFactory::buildDelayMessage(const std::string &topic, const std::string &tags, const std::string &body, int delayTimeLevel)
{
    std::string keys = RandomUtils::getStringByUUID();
    rocketmq::MQMessage msg(topic, tags, keys, body);
    msg.setDelayTimeLevel(delayTimeLevel);
    return msg;
}

// rocketmq::MQMessage MessageFactory::buildOrderMessage(const std::string& topic, const std::string& tags, const std::string& body, const std::string& messageGroup){

// }

rocketmq::MQMessage MessageFactory::buildMessageWithProperty(const std::string &topic, std::map<std::string, std::string> &properties)
{
    std::string body = RandomUtils::getStringByUUID();
    rocketmq::MQMessage msg(topic, body);
    msg.setProperties(properties);
    return msg;
}

rocketmq::MQMessage MessageFactory::buildMessageWithProperty(const std::string &topic, const std::string &messageBody, std::map<std::string, std::string> &properties)
{
    rocketmq::MQMessage msg(topic, messageBody);
    msg.setProperties(properties);
    return msg;
}

// rocketmq::MQMessage MessageFactory::buildOrderMessageWithProperty(const std::string& topic, const std::string& messageBody, const std::string& messageGroup, std::map<std::string,std::string>& properties);

rocketmq::MQMessage MessageFactory::buildMessageWithProperty(const std::string &topic, const std::string &tag, const std::string &body, std::map<std::string, std::string> &properties, const std::vector<std::string> &keys)
{
    rocketmq::MQMessage msg(topic, tag, body);
    msg.setProperties(properties);
    msg.setKeys(keys);
    return msg;
}