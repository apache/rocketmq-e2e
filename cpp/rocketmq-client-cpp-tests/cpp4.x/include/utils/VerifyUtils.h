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
#include <memory>
#include <rocketmq/DefaultMQPullConsumer.h>

class VerifyUtils {
private:
    const static int TIMEOUT = 90;
    static std::vector<std::string> waitForMessageConsume(DataCollector<std::string>& enqueueMessages,DataCollector<std::string>& dequeueMessages,long long timeoutMills, int consumedTimes);
public:
    VerifyUtils() = delete;
    static bool tryReceiveOnce(std::string& topic,std::string& tag, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer);
    static std::vector<rocketmq::MQMessageExt> fetchMessages(std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer, const std::string& topic);
    static bool verifyNormalMessage(DataCollector<std::string>& enqueueMessages, DataCollector<std::string>& dequeueMessages);
};