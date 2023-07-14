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

#include <string>
#include <iostream>
#include <cstdlib>
#include <cstdio>

class MQAdminUtils {
public:
  static std::string getRootPath();

  static std::string executeShellCommand(const std::string& command);

  static std::string createTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver);

  static std::string createDelayTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver);

  static std::string createFIFOTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver);

  static std::string createTransactionTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver);

  static std::string createOrderlyConsumerGroup(const std::string& consumerGroup, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver);

  static std::string clusterList(const std::string& nameserver);
};