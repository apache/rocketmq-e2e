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
#include <unordered_map>
#include <random>
#include <chrono>
#include <openssl/evp.h>
#include <iomanip>

#include "utils/NameUtils.h"

std::string NameUtils::generateRandomAlphanumeric(int length) {
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

std::string NameUtils::generateMD5Sum(const std::string& input) {
    unsigned char hash[MD5_DIGEST_LENGTH];
    unsigned int hash_length;
    
    const EVP_MD* sha256_md = EVP_sha256();
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(mdctx, sha256_md, nullptr);
    EVP_DigestUpdate(mdctx, reinterpret_cast<const unsigned char*>(input.c_str()), input.length());
    EVP_DigestFinal_ex(mdctx, hash, &hash_length);
    EVP_MD_CTX_free(mdctx);

    std::stringstream ss;
    for (unsigned int i = 0; i < hash_length; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }

    return ss.str();
}

std::string NameUtils::getTopicName() {
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

std::string NameUtils::getGroupName() {
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

std::string NameUtils::getTagName() {
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

std::string NameUtils::getRandomTopicName() {
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

std::string NameUtils::getTopicName(const std::string& messageType, const std::string& className, const std::string& methodName) {
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

std::string NameUtils::getRandomTopicName(const std::string& suffix) {
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

std::string NameUtils::getRandomGroupName() {
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

std::string NameUtils::getGroupName(const std::string& className, const std::string& methodName) {
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

std::string NameUtils::getRandomGroupName(const std::string& suffix) {
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

std::string NameUtils::getRandomTagName() {
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