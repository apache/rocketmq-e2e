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
#ifndef ROCKETMQ_CLIENT_CPP_TESTS_SERVER_DELAYMESSAGETEST_H_
#define ROCKETMQ_CLIENT_CPP_TESTS_SERVER_DELAYMESSAGETEST_H_

#include "rocketmq/PushConsumer.h"
#include "rocketmq/Producer.h"
#include "rocketmq/SimpleConsumer.h"
#include "utils/NameUtils.h"

class DelayMessage
{
private:
    std::string tag;
    const int SEND_NUM=10;
public:
    DelayMessage() {this->tag = NameUtils::getRandomTagName();}
    ~DelayMessage() {}
    std::string getTag() { return this->tag; }
    int getSendNum() { return this->SEND_NUM; }
    // testDelay_Send_PushConsume();
    // testDelay_SendAsync_PushConsume();
    // testDelayTime15SecondsAgo();
    // testDelayTime24hAfter();
};

class DelayMessageTest:public testing::Test
{
protected:
    DelayMessage* d;
    virtual void SetUp() {
        d = new DelayMessage();
    }
    virtual void TearDown() {
        delete d;
    }
};


#endif  // ROCKETMQ_CLIENT_CPP_TESTS_SERVER_DELAYMESSAGETEST_H_
