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
#include <memory>
#include <rocketmq/DefaultMQProducer.h>
#include <spdlog/logger.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class ProducerFactory {
public:
    ProducerFactory()=delete;


    static std::shared_ptr<rocketmq::DefaultMQProducer> getProducer(std::string group){
        auto producer = std::make_shared<rocketmq::DefaultMQProducer>(group);
        producer->setNamesrvAddr(resource->getNamesrv());
        producer->setInstanceName(group);
        producer->setTcpTransportTryLockTimeout(1000);
        producer->setTcpTransportConnectTimeout(400);
        producer->start();
        return producer;
    }
};