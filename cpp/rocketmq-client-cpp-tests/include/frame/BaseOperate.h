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
#ifndef ROCKETMQ_CLIENT_CPP_FRAME_BASEOPERATE_H_
#define ROCKETMQ_CLIENT_CPP_FRAME_BASEOPERATE_H_

#include <string>
#include <memory>
#include <random>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <cassert>
#include <chrono>


#include "utils/MQAdminUtils.h"
#include "utils/RandomUtils.h"

std::string getTopic(const std::string& messageType, const std::string& methodName,const std::string& brokerAddr) {
  std::string topic = "topic_" + messageType + "_" + methodName + "_" + RandomUtils::getStringWithCharacter(6);
  std::cout << "\033[0;32m [Topic] topic:" << topic << ", messageType:" << messageType << ", methodName:" << methodName << "\033[0m" << std::endl;
  MQAdminUtils::createTopic(topic, brokerAddr , "", "");
  return topic;
}


std::string getGroupId(std::string methodName) {
  std::string randomStr = "";
  static const char CHARACTERS[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static const int STRING_LENGTH = 6;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, sizeof(CHARACTERS) - 2);
  for (int i = 0; i < STRING_LENGTH; i++) {
    randomStr += CHARACTERS[dis(gen)];
  }
  std::string groupId = "GID_" + methodName + "_" + randomStr;
  std::cout << "[ConsumerGroupId] groupId:" << groupId << ", methodName:" << methodName << std::endl;
  return groupId;
}

#endif