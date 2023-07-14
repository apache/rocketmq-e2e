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

#include <unordered_map>
#include <random>
#include <chrono>
#include <openssl/md5.h>
#include <mutex>

class NameUtils {
private:
    static std::unordered_map<std::string, std::string> alreadyUsed;
    static std::mutex mtx;

    static std::string generateRandomAlphanumeric(int length);
    static std::string generateMD5Sum(const std::string& input);

public:
    NameUtils() = delete;
    static std::string getTopicName();
    static std::string getGroupName();
    static std::string getTagName();
    static std::string getRandomTopicName();
    static std::string getTopicName(const std::string& messageType, const std::string& className, const std::string& methodName);
    static std::string getRandomTopicName(const std::string& suffix);
    static std::string getRandomGroupName();
    static std::string getGroupName(const std::string& className, const std::string& methodName);
    static std::string getRandomGroupName(const std::string& suffix);
    static std::string getRandomTagName();
};