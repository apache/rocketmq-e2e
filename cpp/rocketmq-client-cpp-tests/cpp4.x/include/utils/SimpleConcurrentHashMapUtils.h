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

template <typename Key, typename Value>
class SimpleConcurrentHashMap
{
private:
    std::unordered_map<Key, Value> map_;
    mutable std::shared_mutex mutex_;

public:
    size_t size() const
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        return map_.size();
    }

    void insert(const Key &key, const Value &value)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        map_[key] = value;
    }

    bool contains(const Key &key) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return (map_.find(key) != map_.end());
    }

    void update(const Key &key, const Value &value)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        map_[key] = value;
    }

    std::vector<Value> getAllValues() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<Value> values;
        values.reserve(map_.size());
        for (const auto &entry : map_)
        {
            values.push_back(entry.second);
        }
        return values;
    }

    Value &operator[](const Key &key)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        return map_[key];
    }
};

template <typename Key, typename Value>
class SimpleConcurrentHashMap<Key, std::atomic<Value>>
{
private:
    std::unordered_map<Key, Value> map_;
    mutable std::shared_mutex mutex_;

public:
    size_t size() const
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        return map_.size();
    }

    void insert(const Key &key, const Value &value)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        map_[key] = value;
    }

    bool contains(const Key &key) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return (map_.find(key) != map_.end());
    }

    void update(const Key &key, const Value &value)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        map_[key] = value;
    }

    std::vector<Value> getAllValues() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<Value> values;
        values.reserve(map_.size());
        for (const auto &entry : map_)
        {
            values.push_back(entry.second);
        }
        return values;
    }

    Value &operator[](const Key &key)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        return map_[key];
    }
};
