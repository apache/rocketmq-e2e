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

class Resource{
private:
    std::string namesrv;
    std::string brokerAddr;
    std::string cluster;
    std::string accessKey;
    std::string secretKey;
    std::string accessChannel;
public:
    Resource() {}

    Resource(std::string namesrv, std::string brokerAddr, std::string cluster, std::string accessKey, std::string secretKey, std::string accessChannel)
        : namesrv(namesrv),
          brokerAddr(brokerAddr),
          cluster(cluster),
          accessKey(accessKey),
          secretKey(secretKey),
          accessChannel(accessChannel) {
          }
    
    //set
    void setNamesrv(std::string namesrv) {
        this->namesrv = namesrv;
    }
    void setBrokerAddr(std::string brokerAddr) {
        this->brokerAddr = brokerAddr;
    }
    void setCluster(std::string cluster) {
        this->cluster = cluster;
    }
    void setAccessKey(std::string accessKey) {
        this->accessKey = accessKey;
    }
    void setSecretKey(std::string secretKey) {
        this->secretKey = secretKey;
    }
    void setAccessChannel(std::string accessChannel) {
        this->accessChannel = accessChannel;
    }

    //get
    std::string getNamesrv() {
        return namesrv;
    }
    std::string getBrokerAddr() {
        return brokerAddr;
    }
    std::string getCluster() {
        return cluster;
    }
    std::string getAccessKey() {
        return accessKey;
    }
    std::string getSecretKey() {
        return secretKey;
    }
    std::string getAccessChannel() {
        return accessChannel;
    }
};