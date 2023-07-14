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
#include <string.h>
#include "rocketmq/DefaultMQPushConsumer.h"

extern std::string namesrv;
extern std::string brokerAddr;
extern std::string cluster;

TEST(PushConsumerInitTest, testNormalSetting){
    std::string groupId = getGroupId("testNormalSetting");
    std::string topic = getTopic(MessageType::NORMAL, "testNormalSetting", brokerAddr,namesrv,cluster);
    rocketmq::DefaultMQPushConsumer consumer(groupId);
    consumer.setNamesrvAddr(namesrv);
    consumer.setGroupName(groupId);
    consumer.setConsumeThreadCount(20);
    consumer.subscribe(topic, "*");
    consumer.setTcpTransportTryLockTimeout(1000);
    consumer.setTcpTransportConnectTimeout(400);    

    if (info.syncpush)
      consumer.setAsyncPull(false);  // set sync pull
    if (info.broadcasting) {
      consumer.setMessageModel(rocketmq::BROADCASTING);
    }   
    consumer.setInstanceName(info.groupname);   
    
    MyMsgListener msglistener;
    consumer.registerMessageListener(&msglistener); 
    try {
      consumer.start();
    } catch (MQClientException& e) {
      cout << e << endl;
    }
}