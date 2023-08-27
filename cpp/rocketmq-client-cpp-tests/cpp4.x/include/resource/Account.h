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

class Account {
private:
    // aliyun AccessKey
    std::string accessKey;
    // aliyun SecretKey
    std::string secretKey;
    // instanceUserName
    std::string instanceUserName;
    // instancePassword
    std::string instancePassword;
    // endpoint
    std::string endpoint;
    // instanceId
    std::string instanceId;
    // consoleEndpoint
    std::string consoleEndpoint;
    // region
    std::string regionId;
    // Account User ID
    std::string userId;
    // Account User Name
    std::string accountName;
public:
    Account() {}
    Account(std::string endpoint) : endpoint(endpoint) {}

    Account(std::string instanceUserName, std::string instancePassword)
        : instanceUserName(instanceUserName),
          instancePassword(instancePassword) {}

    Account(std::string accessKey, std::string secretKey, std::string endpoint)
        : accessKey(accessKey),
          secretKey(secretKey),
          endpoint(endpoint) {}

    Account(std::string accessKey, std::string secretKey, std::string endpoint, 
            std::string instanceId, std::string consoleEndpoint, std::string regionId, 
            std::string userId)
        : accessKey(accessKey),
          secretKey(secretKey),
          endpoint(endpoint),
          instanceId(instanceId),
          consoleEndpoint(consoleEndpoint),
          regionId(regionId),
          userId(userId) {}
    
    std::string getAccessKey() const { return accessKey; }
    void setAccessKey(const std::string& accessKey) { accessKey = accessKey; }

    std::string getSecretKey() const { return secretKey; }
    void setSecretKey(const std::string& secretKey) { secretKey = secretKey; }

    std::string getInstanceUserName() const { return instanceUserName; }
    void setInstanceUserName(const std::string& instanceUserName) { instanceUserName = instanceUserName; }

    std::string getInstancePassword() const { return instancePassword; }
    void setInstancePassword(const std::string& instancePassword) { instancePassword = instancePassword; }

    std::string getEndpoint() const { return endpoint; }
    void setEndpoint(const std::string& endpoint) { endpoint = endpoint; }

    std::string getInstanceId() const { return instanceId; }
    void setInstanceId(const std::string& instanceId) { instanceId = instanceId; }

    std::string getConsoleEndpoint() const { return consoleEndpoint; }
    void setConsoleEndpoint(const std::string& consoleEndpoint) { consoleEndpoint = consoleEndpoint; }

    std::string getRegionId() const { return regionId; }
    void setRegionId(const std::string& regionId) { regionId = regionId; }

    std::string getUserId() const { return userId; }
    void setUserId(const std::string& userId) { userId = userId; }

    std::string getAccountName() const { return accountName; }
    void setAccountName(const std::string& accountName) { accountName = accountName; }
};