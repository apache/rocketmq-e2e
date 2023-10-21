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

#include <iostream>
#include <unordered_map>
#include <shared_mutex>
#include <functional>
#include <vector>
#include <atomic>

template <typename Value>
class SimpleConcurrentVector
{
private:
    std::vector<Value> v_;
    mutable std::shared_mutex mutex_;

public:
    size_t size() const
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        return v_.size();
    }

    void push_back(const Value &value)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        v_.push_back(value);
    }

    void insert(const size_t &index, const Value &value)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        v_.insert(v_.begin() + index, value);
    }

    void erase(const size_t &index)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        v_.erase(v_.begin() + index);
    }

    std::vector<Value> getCopy() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return v_;
    }

    Value &operator[](const size_t &index)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return v_[index];
    }
};
