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
#include <mutex>
#include <memory>
#include <string>
#include "utils/data/collect/DataCollector.h"
#include "utils/data/collect/impl/ListDataCollector.h"
#include "utils/data/collect/impl/MapDataCollector.h"

template <typename T>
class DataCollectorManager {
private:
    static std::unique_ptr<DataCollectorManager<T>> instance;
    std::unordered_map<std::string, std::unique_ptr<DataCollector<T>>> collectMap;
    static std::mutex mtx;

    DataCollectorManager() {}
public:
    static DataCollectorManager& getInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mtx);
            if (instance == nullptr)
                instance = std::unique_ptr<DataCollectorManager<T>>(new DataCollectorManager<T>());
        }
        return *instance;
    }

    DataCollector<T>& fetchDataCollector(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);

        if (collectMap.find(key) == collectMap.end()) {
            collectMap[key] = std::make_unique<MapDataCollectorImpl<T>>();
        }

        return *collectMap[key];
    }

    DataCollector<T>& fetchMapDataCollector(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        
        if (collectMap.find(key) == collectMap.end() || 
            dynamic_cast<MapDataCollectorImpl<T>*>(collectMap[key].get()) == nullptr) {
            collectMap[key] = std::make_unique<MapDataCollectorImpl<T>>();
        }

        return *collectMap[key];
    }

    DataCollector<T>& fetchListDataCollector(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);

        if (collectMap.find(key) == collectMap.end() ||
            dynamic_cast<ListDataCollectorImpl<T>*>(collectMap[key].get()) == nullptr) {
            collectMap[key] = std::make_unique<ListDataCollectorImpl<T>>();
        }

        return *collectMap[key];
    }

    void resetDataCollect(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        
        if (collectMap.find(key) != collectMap.end()) {
            collectMap[key]->resetData();
        }
    }

    void resetAll() {
        std::lock_guard<std::mutex> lock(mtx);

        for (auto& pair : collectMap) {
            pair.second->resetData();
        }
    }

    void removeDataCollect(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        
        collectMap.erase(key);
    }

    void removeAll() {
        std::lock_guard<std::mutex> lock(mtx);

        collectMap.clear();
    }
};

template<typename T>
std::mutex DataCollectorManager<T>::mtx;

// class DataCollectorManager {
// private:
//     static std::unique_ptr<DataCollectorManager> instance;
//     std::unordered_map<std::string, std::unique_ptr<DataCollector>> collectMap;
//     static std::mutex mtx;

//     DataCollectorManager() {}
// public:
//     static DataCollectorManager& getInstance() {
//         if (instance == nullptr) {
//             std::lock_guard<std::mutex> lock(mtx);
//             if (instance == nullptr)
//                 instance = std::unique_ptr<DataCollectorManager>(new DataCollectorManager());
//         }
//         return *instance;
//     }

//     DataCollector& fetchDataCollector(const std::string& key) {
//         std::lock_guard<std::mutex> lock(mtx);

//         if (collectMap.find(key) == collectMap.end()) {
//             collectMap[key] = std::make_unique<MapDataCollectorImpl>();
//         }

//         return *collectMap[key];
//     }

//     DataCollector& fetchMapDataCollector(const std::string& key) {
//         std::lock_guard<std::mutex> lock(mtx);
        
//         if (collectMap.find(key) == collectMap.end() || 
//             dynamic_cast<MapDataCollectorImpl*>(collectMap[key].get()) == nullptr) {
//             collectMap[key] = std::make_unique<MapDataCollectorImpl>();
//         }

//         return *collectMap[key];
//     }

//     DataCollector& fetchListDataCollector(const std::string& key) {
//         std::lock_guard<std::mutex> lock(mtx);

//         if (collectMap.find(key) == collectMap.end() ||
//             dynamic_cast<ListDataCollectorImpl*>(collectMap[key].get()) == nullptr) {
//             collectMap[key] = std::make_unique<ListDataCollectorImpl>();
//         }

//         return *collectMap[key];
//     }

//     void resetDataCollect(const std::string& key) {
//         std::lock_guard<std::mutex> lock(mtx);
        
//         if (collectMap.find(key) != collectMap.end()) {
//             collectMap[key]->resetData();
//         }
//     }

//     void resetAll() {
//         std::lock_guard<std::mutex> lock(mtx);

//         for (auto& pair : collectMap) {
//             pair.second->resetData();
//         }
//     }

//     void removeDataCollect(const std::string& key) {
//         std::lock_guard<std::mutex> lock(mtx);
        
//         collectMap.erase(key);
//     }

//     void removeAll() {
//         std::lock_guard<std::mutex> lock(mtx);

//         collectMap.clear();
//     }
// };