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
#include <string>
#include "utils/RandomUtils.h"
#include "utils/data/collect/DataCollector.h"
#include "utils/data/collect/DataCollectorManager.h"

template <typename T>
class MQCollector
{
protected:
    DataCollector<T> *enqueueMessages;
    DataCollector<T> *enqueueFailedMessages;
    DataCollector<T> *dequeueMessages;
    DataCollector<T> *dequeueAllMessages;
    DataCollector<T> *dequeueUndupMessageBody;
    DataCollector<T> *msgRTs;

public:
    MQCollector()
    {
        enqueueMessages = &DataCollectorManager<T>::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
        enqueueFailedMessages = &DataCollectorManager<T>::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
        dequeueMessages = &DataCollectorManager<T>::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
        dequeueAllMessages = &DataCollectorManager<T>::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
        dequeueUndupMessageBody = &DataCollectorManager<T>::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
        msgRTs = &DataCollectorManager<T>::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
    }

    void clearMsg()
    {
        enqueueMessages->resetData();
        enqueueFailedMessages->resetData();
        dequeueMessages->resetData();
        dequeueAllMessages->resetData();
        dequeueUndupMessageBody->resetData();
        msgRTs->resetData();
    }

    void lockCollectors()
    {
        enqueueMessages->lockIncrement();
        enqueueFailedMessages->lockIncrement();
        dequeueMessages->lockIncrement();
        dequeueAllMessages->lockIncrement();
        dequeueUndupMessageBody->lockIncrement();
        msgRTs->lockIncrement();
    }

    DataCollector<T> *getEnqueueMessages()
    {
        return enqueueMessages;
    }

    DataCollector<T> *getEnqueueFailedMessages()
    {
        return enqueueFailedMessages;
    }

    DataCollector<T> *getDequeueMessages()
    {
        return dequeueMessages;
    }

    DataCollector<T> *getDequeueAllMessages()
    {
        return dequeueAllMessages;
    }

    DataCollector<T> *getDequeueUndupMessageBody()
    {
        return dequeueUndupMessageBody;
    }
};

// class MQCollector {
// protected:
//     DataCollector* enqueueMessages;
//     DataCollector* enqueueFailedMessages;
//     DataCollector* dequeueMessages;
//     DataCollector* dequeueAllMessages;
//     DataCollector* dequeueUndupMessageBody;
//     DataCollector* msgRTs;

// public:
//     MQCollector() {
//         enqueueMessages = &DataCollectorManager::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
//         enqueueFailedMessages = &DataCollectorManager::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
//         dequeueMessages = &DataCollectorManager::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
//         dequeueAllMessages = &DataCollectorManager::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
//         dequeueUndupMessageBody = &DataCollectorManager::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());
//         msgRTs = &DataCollectorManager::getInstance().fetchDataCollector(RandomUtils::getStringByUUID());

//     }

//     void clearMsg() {
//         enqueueMessages->resetData();
//         enqueueFailedMessages->resetData();
//         dequeueMessages->resetData();
//         dequeueAllMessages->resetData();
//         dequeueUndupMessageBody->resetData();
//         msgRTs->resetData();
//     }

//     void lockCollectors() {
//         enqueueMessages->lockIncrement();
//         enqueueFailedMessages->lockIncrement();
//         dequeueMessages->lockIncrement();
//         dequeueAllMessages->lockIncrement();
//         dequeueUndupMessageBody->lockIncrement();
//         msgRTs->lockIncrement();
//     }

//     DataCollector* getEnqueueMessages() {
//         return enqueueMessages;
//     }

//     DataCollector* getEnqueueFailedMessages() {
//         return enqueueFailedMessages;
//     }

//     DataCollector* getDequeueMessages() {
//         return dequeueMessages;
//     }

//     DataCollector* getDequeueAllMessages() {
//         return dequeueAllMessages;
//     }

//     DataCollector* getDequeueUndupMessageBody() {
//         return dequeueUndupMessageBody;
//     }

// };
