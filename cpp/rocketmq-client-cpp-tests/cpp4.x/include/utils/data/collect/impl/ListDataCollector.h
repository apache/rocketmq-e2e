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
#include <algorithm>
#include <set>
#include <vector>
#include <mutex>
#include <string>
#include "utils/data/collect/DataCollector.h"

template <typename T>
class ListDataCollectorImpl : public DataCollector<T>
{
private:
    std::vector<T> data;
    bool lock = false;
    std::mutex mtx;

public:
    ListDataCollectorImpl() {}

    ListDataCollectorImpl(const std::vector<T> &initial_data)
    {
        for (const std::string &item : initial_data)
        {
            addData(item);
        }
    }

    void resetData() override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        data.clear();
        unlockIncrement();
    }

    std::vector<T> getAllData() override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        return std::vector<T>(data.begin(), data.end());
    }

    std::set<T> getAllDataWithoutDuplicate() override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        return std::set<T>(data.begin(), data.end());
    }

    void addData(const T &new_data) override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        if (!lock)
        {
            data.push_back(new_data);
        }
    }

    size_t getDataSizeWithoutDuplicate() override
    {
        std::lock_guard<std::mutex> lock_thislock(mtx);
        return getAllDataWithoutDuplicate().size();
    }

    size_t getDataSize() override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        return data.size();
    }

    bool isRepeatedData(const T &data) override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        return std::count(this->data.begin(), this->data.end(), data) > 1;
    }

    int getRepeatedTimeForData(const T &data) override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        return std::count(this->data.begin(), this->data.end(), data);
    }

    void removeData(const T &data) override
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        this->data.erase(std::remove(this->data.begin(), this->data.end(), data), this->data.end());
    }

    void lockIncrement() override
    {
        lock = true;
    }

    void unlockIncrement() override
    {
        lock = false;
    }

    std::string getFirstElement()
    {
        std::lock_guard<std::mutex> lock_this(mtx);
        return data.front();
    }
};