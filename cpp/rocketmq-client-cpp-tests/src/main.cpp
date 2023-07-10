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
#include <gtest/gtest.h>
#include <iostream>

#include "rocketmq/Logger.h"
#include "test/server/DelayMessageTest.h"
#include "utils/NameUtils.h"

std::unordered_map<std::string, std::string> NameUtils::alreadyUsed;
std::mutex NameUtils::mtx;

int main(int argc,char* argv[])
{
    // Adjust log level for file/console sinks
    // auto& logger = ROCKETMQ_NAMESPACE::getLogger();
    // logger.setConsoleLevel(ROCKETMQ_NAMESPACE::Level::Info);
    // logger.setLevel(ROCKETMQ_NAMESPACE::Level::Info);
    // logger.init();

    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}