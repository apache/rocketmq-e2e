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
#ifndef ROCKETMQ_CLIENT_CPP_TESTS_MQADMINUTILS_H_
#define ROCKETMQ_CLIENT_CPP_TESTS_MQADMINUTILS_H_

#include <string>
#include <iostream>
#include <cstdlib>
#include <cstdio>

class MQAdminUtils {
public:
  static std::string getRootPath() {
    std::string projectBasePath = std::getenv("PWD");
    std::string path = projectBasePath;
    path = path.substr(0, path.find_last_of("/"));
    path = path.substr(0, path.find_last_of("/"));
    path = path.substr(0, path.find_last_of("/"));
    path = path.substr(0, path.find_last_of("/"));
    return path;
  }

  static std::string executeShellCommand(const std::string& command) {
    std::string output;
    char buffer[128];
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
      return "ERROR";
    }
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
      output += buffer;
    }
    pclose(pipe);
    std::cout << output << std::endl;
    return output;
  }

  static std::string createTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver) {
    // use absolute path
    std::string path = getRootPath();
    std::string command = "sh " + path + "/common/bin/mqadmin updateTopic -t " + topicName;
    if (!nameserver.empty()) {
      command = command + " -n " + nameserver;
    }
    if (!brokerAddr.empty()) {
      command = command + " -b " + brokerAddr;
    }
    if (!clusterName.empty()) {
      command = command + " -c " + clusterName;
    }
    std::cout << command << std::endl;
    return executeShellCommand(command);
  }

  static std::string createDelayTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver) {
    // use absolute path
    std::string path = getRootPath();
    std::string command = "sh " + path + "/common/bin/mqadmin updateTopic -t " + topicName;
    if (!nameserver.empty()) {
      command = command + " -n " + nameserver;
    }
    if (!brokerAddr.empty()) {
      command = command + " -b " + brokerAddr;
    }
    if (!clusterName.empty()) {
      command = command + " -c " + clusterName;
    }
    command = command + " -a " + "+message.type=DELAY";
    return executeShellCommand(command);
  }

  static std::string createFIFOTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver) {
    // use absolute path
    std::string path = getRootPath();
    std::string command = "sh " + path + "/common/bin/mqadmin updateTopic -t " + topicName;
    if (!nameserver.empty()) {
      command = command + " -n " + nameserver;
    }
    if (!brokerAddr.empty()) {
      command = command + " -b " + brokerAddr;
    }
    if (!clusterName.empty()) {
      command = command + " -c " + clusterName;
    }
    command = command + " -a " + "+message.type=FIFO";
    return executeShellCommand(command);
  }

  static std::string createTransactionTopic(const std::string& topicName, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver) {
    // use absolute path
    std::string path = getRootPath();
    std::string command = "sh " + path + "/common/bin/mqadmin updateTopic -t " + topicName;
    if (!nameserver.empty()) {
      command = command + " -n " + nameserver;
    }
    if (!brokerAddr.empty()) {
      command = command + " -b " + brokerAddr;
    }
    if (!clusterName.empty()) {
      command = command + " -c " + clusterName;
    }
    command = command + " -a " + "+message.type=TRANSACTION";
    return executeShellCommand(command);
  }

  static std::string createOrderlyConsumerGroup(const std::string& consumerGroup, const std::string& brokerAddr, const std::string& clusterName, const std::string& nameserver) {
    // use absolute path
    std::string path = getRootPath();
    std::string command = "sh " + path + "/common/bin/mqadmin updateSubGroup -g " + consumerGroup;
    if (!nameserver.empty()) {
      command = command + " -n " + nameserver;
    }
    if (!brokerAddr.empty()) {
      command = command + " -b " + brokerAddr;
    }
    if (!clusterName.empty()) {
      command = command + " -c " + clusterName;
    }
    command = command + " -s true -o true -m false -d false ";
    return executeShellCommand(command);
  }

  static std::string clusterList(const std::string& nameserver) {
    std::string path = getRootPath();
    std::string command = "sh " + path + "/common/bin/mqadmin clusterlist";
    if (!nameserver.empty()) {
      command = command + " -n " + nameserver;
    }
    return executeShellCommand(command);
  }
};

#endif