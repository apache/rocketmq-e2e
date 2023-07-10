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
#ifndef ROCKETMQ_CLIENT_CPP_TESTS_UTILS_NAMUTILS_H_
#define ROCKETMQ_CLIENT_CPP_TESTS_UTILS_NAMUTILS_H_

#include <unordered_map>
#include <random>
#include <chrono>
#include <openssl/md5.h>
#include <mutex>

class NameUtils {
private:
    static std::unordered_map<std::string, std::string> alreadyUsed;
    static std::mutex mtx;

    static std::string generateRandomAlphanumeric(int length) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 35);
        std::string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        std::string result;
        for (int i = 0; i < length; i++) {
            result += chars[dis(gen)];
        }
        return result;
    }

    static std::string generateMD5Sum(const std::string& input) {
        unsigned char hash[MD5_DIGEST_LENGTH];
        MD5(reinterpret_cast<const unsigned char*>(input.c_str()), input.length(), hash);
        std::string md5sum;
        for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
            md5sum += hash[i];
        }
        return md5sum;
    }

public:
    static std::string getTopicName() {
        while (true) {
            std::string topic = "topic-server-" + generateRandomAlphanumeric(20);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(topic);
            if (it == alreadyUsed.end()) {
                alreadyUsed[topic] = topic;
                return topic;
            }
        }
    }

    static std::string getGroupName() {
        while (true) {
            std::string gid = "GID-server-" + generateRandomAlphanumeric(20);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(gid);
            if (it == alreadyUsed.end()) {
                alreadyUsed[gid] = gid;
                return gid;
            }
        }
    }

    static std::string getTagName() {
        while (true) {
            std::string tag = "tag-server-" + generateRandomAlphanumeric(20);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(tag);
            if (it == alreadyUsed.end()) {
                alreadyUsed[tag] = tag;
                return tag;
            }
        }
    }

    static std::string getRandomTopicName() {
        while (true) {
            std::string topic = "topic-server-" + generateRandomAlphanumeric(20);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(topic);
            if (it == alreadyUsed.end()) {
                alreadyUsed[topic] = topic;
                return topic;
            }
        }
    }

    static std::string getTopicName(const std::string& messageType, const std::string& className, const std::string& methodName) {
        while (true) {
            std::string topic = "topic-" + messageType + "-" + generateMD5Sum(className + methodName);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(topic);
            if (it == alreadyUsed.end()) {
                alreadyUsed[topic] = topic;
                return topic;
            }
        }
    }

    static std::string getRandomTopicName(const std::string& suffix) {
        while (true) {
            std::string topic = "topic-" + generateRandomAlphanumeric(6) + "-" + suffix;
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(topic);
            if (it == alreadyUsed.end()) {
                alreadyUsed[topic] = topic;
                return topic;
            }
        }
    }

    static std::string getRandomGroupName() {
        while (true) {
            std::string gid = "GID-server-" + generateRandomAlphanumeric(20);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(gid);
            if (it == alreadyUsed.end()) {
                alreadyUsed[gid] = gid;
                return gid;
            }
        }
    }

    static std::string getGroupName(const std::string& className, const std::string& methodName) {
        while (true) {
            std::string gid = "GID-" + generateMD5Sum(className + methodName);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(gid);
            if (it == alreadyUsed.end()) {
                alreadyUsed[gid] = gid;
                return gid;
            }
        }
    }

    static std::string getRandomGroupName(const std::string& suffix) {
        while (true) {
            std::string gid = "GID-" + generateRandomAlphanumeric(6) + "-" + suffix;
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(gid);
            if (it == alreadyUsed.end()) {
                alreadyUsed[gid] = gid;
                return gid;
            }
        }
    }

    static std::string getRandomTagName() {
        while (true) {
            std::string tag = "tag-server-" + generateRandomAlphanumeric(20);
            std::lock_guard<std::mutex> lock(mtx);
            auto it = alreadyUsed.find(tag);
            if (it == alreadyUsed.end()) {
                alreadyUsed[tag] = tag;
                return tag;
            }
        }
    }
};



#endif  // ROCKETMQ_CLIENT_CPP_TESTS_UTILS_NAMUTILS_H_