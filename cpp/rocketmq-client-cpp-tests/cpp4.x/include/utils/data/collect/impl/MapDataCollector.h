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
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include "utils/data/collect/DataCollector.h"

template <typename T>
class MapDataCollectorImpl : public DataCollector<T> {
    std::unordered_map<T, std::atomic<int>> data;
    bool lock = false;
    mutable std::mutex mtx;

public:
    MapDataCollectorImpl() {}

    MapDataCollectorImpl(const std::vector<T>& initial_data) {
        for (const std::string& item : initial_data) {
            addData(item);
        }
    }

    void resetData() override {
        std::lock_guard<std::mutex> lock_this(mtx);
        data.clear();
        unlockIncrement();
    }

    std::vector<T> getAllData() override {
        std::lock_guard<std::mutex> lock_this(mtx);
        std::vector<T> result;
        for (const auto& pair : data) {
            result.insert(result.end(), pair.second, pair.first);
        }
        return result;
    }

    size_t getDataSizeWithoutDuplicate() override {
        std::lock_guard<std::mutex> lock_this(mtx);
        return data.size();
    }

    void addData(const T& new_data) override {
        std::lock_guard<std::mutex> lock_this(mtx);
        if (!lock) {
            data[new_data]++;
        }
    }

    size_t getDataSize() override {
        std::lock_guard<std::mutex> lock_this(mtx);
        size_t count = 0;
        for (const auto& pair : data) {
            count += pair.second;
        }
        return count;
    }

    bool isRepeatedData(const T& data) override {
        std::lock_guard<std::mutex> lock_this(mtx);
        return this->data[data] > 1;
    }

    std::set<T> getAllDataWithoutDuplicate() override {
        std::lock_guard<std::mutex> lock_this(mtx);
        std::set<T> result;
        for (const auto& pair : data) {
            result.insert(pair.first);
        }
        return result;
    }

    int getRepeatedTimeForData(const T& data) override {
        std::lock_guard<std::mutex> lock_this(mtx);
        return this->data[data];
    }

    void removeData(const T& data) override {
        std::lock_guard<std::mutex> lock_this(mtx);
        this->data.erase(data);
    }

    void lockIncrement() override {
        lock = true;
    }

    void unlockIncrement() override {
        lock = false;
    }
};

// class MapDataCollectorImpl : public DataCollector {
//     std::unordered_map<std::string, std::atomic<int>> data;
//     bool lock = false;
//     mutable std::mutex mtx;

// public:
//     MapDataCollectorImpl() {}

//     MapDataCollectorImpl(const std::vector<std::string>& initial_data) {
//         for (const std::string& item : initial_data) {
//             addData(item);
//         }
//     }

//     void resetData() override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         data.clear();
//         unlockIncrement();
//     }

//     std::vector<std::string> getAllData() override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         std::vector<std::string> result;
//         for (const auto& pair : data) {
//             result.insert(result.end(), pair.second, pair.first);
//         }
//         return result;
//     }

//     size_t getDataSizeWithoutDuplicate() override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         return data.size();
//     }

//     void addData(const std::string& new_data) override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         if (!lock) {
//             data[new_data]++;
//         }
//     }

//     size_t getDataSize() override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         size_t count = 0;
//         for (const auto& pair : data) {
//             count += pair.second;
//         }
//         return count;
//     }

//     bool isRepeatedData(const std::string& data) override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         return this->data[data] > 1;
//     }

//     std::set<std::string> getAllDataWithoutDuplicate() override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         std::set<std::string> result;
//         for (const auto& pair : data) {
//             result.insert(pair.first);
//         }
//         return result;
//     }

//     int getRepeatedTimeForData(const std::string& data) override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         return this->data[data];
//     }

//     void removeData(const std::string& data) override {
//         std::lock_guard<std::mutex> lock_this(mtx);
//         this->data.erase(data);
//     }

//     void lockIncrement() override {
//         lock = true;
//     }

//     void unlockIncrement() override {
//         lock = false;
//     }
// };