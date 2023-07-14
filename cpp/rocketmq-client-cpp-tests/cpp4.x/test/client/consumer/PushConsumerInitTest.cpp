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
#include "frame/BaseOperate.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <mutex>
#include "rocketmq/DefaultMQPushConsumer.h"

extern std::string namesrv;
extern std::string brokerAddr;
extern std::string cluster;

class MsgListener : public rocketmq::MessageListenerConcurrently {
 public:
  MsgListener() {}
  virtual ~MsgListener() {}

  virtual rocketmq::ConsumeStatus consumeMessage(const std::vector<rocketmq::MQMessageExt>& msgs) {
    return rocketmq::CONSUME_SUCCESS;
  }
};

TEST(PushConsumerInitTest, testNormalSetting){
    SCOPED_TRACE("Start [PushConsumer] failed, expected success.");
    std::string groupId = getGroupId("testNormalSetting");
    std::string topic = getTopic(MessageType::NORMAL, "testNormalSetting", brokerAddr,namesrv,cluster);
    ASSERT_NO_FATAL_FAILURE({
        rocketmq::DefaultMQPushConsumer consumer(groupId);
        consumer.setNamesrvAddr(namesrv);
        consumer.setGroupName(groupId);
        consumer.setConsumeThreadCount(20);
        consumer.setConsumeMessageBatchMaxSize(4 * 1024 * 1024);
        consumer.subscribe(topic, "*");
        MsgListener msglistener;
        consumer.registerMessageListener(&msglistener); 

        consumer.start();
        consumer.shutdown();
    });
}