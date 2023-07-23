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
#include <set>
#include <vector>
#include <string>

template <typename T>
class DataCollector {
public:
    virtual void resetData() = 0;
    virtual std::vector<T> getAllData() = 0;
    virtual std::set<T> getAllDataWithoutDuplicate() = 0;
    virtual void addData(const T& data) = 0;
    virtual std::size_t getDataSizeWithoutDuplicate() = 0;
    virtual std::size_t getDataSize() = 0;
    virtual bool isRepeatedData(const T& data) = 0;
    virtual int getRepeatedTimeForData(const T& data) = 0;
    virtual void removeData(const T& data) = 0;
    virtual void lockIncrement() = 0;
    virtual void unlockIncrement() = 0;
};

// class DataCollector {
// public:
//     virtual void resetData() = 0;
//     virtual std::vector<std::string> getAllData() = 0;
//     virtual std::set<std::string> getAllDataWithoutDuplicate() = 0;
//     virtual void addData(const std::string& data) = 0;
//     virtual std::size_t getDataSizeWithoutDuplicate() = 0;
//     virtual std::size_t getDataSize() = 0;
//     virtual bool isRepeatedData(const std::string& data) = 0;
//     virtual int getRepeatedTimeForData(const std::string& data) = 0;
//     virtual void removeData(const std::string& data) = 0;
//     virtual void lockIncrement() = 0;
//     virtual void unlockIncrement() = 0;
// };