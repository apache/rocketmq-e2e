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
#include <rocketmq/MQMessageExt.h>

class MQMsg : public rocketmq::MQMessageExt {
public:
    MQMsg() : MQMessageExt() {}

    MQMsg(int queueId,
        int64 bornTimestamp,
        sockaddr bornHost,
        int64 storeTimestamp,
        sockaddr storeHost,
        const std::string& msgId,
        const std::string& property)
            :MQMessageExt(queueId, bornTimestamp, bornHost, storeTimestamp, storeHost, msgId),property_(property) {}

    MQMsg(const MQMessageExt& msg, const std::string& property)
        :MQMessageExt(msg),property_(property) {}
    
    MQMsg(const MQMessageExt& msg)
        :MQMessageExt(msg) {}

    MQMsg(const MQMsg& other):MQMessageExt(other),property_(other.property_) {}

    virtual ~MQMsg() {}

    const std::string& getProperty() const {
        return property_;
    }

    void setProperty(const std::string& property) {
        property_ = property;
    }

    bool operator<(const MQMsg& other) const {
        return getMsgId() < other.getMsgId();
    }

private:
    std::string property_;
};