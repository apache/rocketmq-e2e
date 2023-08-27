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
#include <rocketmq/DefaultMQProducer.h>

class MQMessageQueueSelector : public rocketmq::MessageQueueSelector {
public:
    rocketmq::MQMessageQueue select(const std::vector<rocketmq::MQMessageQueue> &mqs, const rocketmq::MQMessage &msg, void *arg) {
        // 实现自定义分区逻辑，根据业务传入arg参数即分区键，计算路由到哪个队列。
        int orderId = *static_cast<int *>(arg);
        int index = orderId % mqs.size();
        return mqs[index];
    }
};