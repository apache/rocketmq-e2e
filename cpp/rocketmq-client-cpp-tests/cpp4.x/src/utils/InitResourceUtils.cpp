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
#include "utils/InitResourceUtils.h"
#include "utils/VerifyUtils.h"
#include "resource/Resource.h"
#include "utils/data/collect/DataCollectorManager.h"
#include "client/rmq/RMQNormalConsumer.h"
#include "utils/NameUtils.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

std::mutex NameUtils::mtx;

std::unordered_map<std::string, std::string> NameUtils::alreadyUsed;

std::atomic<int> RMQNormalConsumer::receivedIndex(0);

std::vector<rocketmq::MQMessageExt> VerifyUtils::msgs;

void initResource(std::shared_ptr<Resource> resource){
    boost::property_tree::ptree pt;
    try {
        boost::property_tree::ini_parser::read_ini("config.ini", pt);
    } catch (boost::property_tree::ini_parser::ini_parser_error& e) {
        multi_logger->info("ini_parser_error: {}",e.what());
    }
    boost::property_tree::ini_parser::read_ini("config.ini", pt);
    resource->setNamesrv(pt.get<std::string>("rocketmq.namesrv"));
    resource->setBrokerAddr(pt.get<std::string>("rocketmq.brokerAddr"));
    resource->setCluster(pt.get<std::string>("rocketmq.cluster"));
    resource->setAccessKey(pt.get<std::string>("rocketmq.accessKey"));
    resource->setSecretKey(pt.get<std::string>("rocketmq.secretKey"));
    resource->setAccessChannel(pt.get<std::string>("rocketmq.accessChannel"));
}