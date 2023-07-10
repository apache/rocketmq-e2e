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
#include <chrono>
#include <iostream>
#include <cassert>

#include <gtest/gtest.h>
#include "test/server/DelayMessageTest.h"
#include "utils/NameUtils.h"
#include "utils/RandomUtils.h"
#include "frame/BaseOperate.h"

TEST_F(DelayMessageTest, testDelay_Send_PushConsume)
{
  std::string access_point = "127.0.0.1:8081";
  std::string methodName = "testDelay_Send_PushConsume";
  std::string topic = getTopic("DELAY", methodName, "127.0.0.1:10911");
  std::string groupId = getGroupId(methodName);

  std::atomic_long receive_count(0);

  auto listener = [&](const ROCKETMQ_NAMESPACE::Message& message) {
    std::cout << "\033[0;33m Received a message[topic=" << message.topic() << ", MsgId=" << message.id() << "]" << "\033[0m" << std::endl;
    receive_count++;
    return ROCKETMQ_NAMESPACE::ConsumeResult::SUCCESS;
  };
  // std::cout << "222222222222222222222222222222222222222222222222222222222222222222222222222222222222"<<std::endl;
  auto push_consumer = ROCKETMQ_NAMESPACE::PushConsumer::newBuilder().withGroup(groupId).withConfiguration(ROCKETMQ_NAMESPACE::Configuration::newBuilder().withEndpoints(access_point).build()).withConsumeThreads(1).withListener(listener).subscribe(topic, d->getTag()).build();
  
  // auto simple_consumer = ROCKETMQ_NAMESPACE::SimpleConsumer::newBuilder().withGroup(groupId).withConfiguration(ROCKETMQ_NAMESPACE::Configuration::newBuilder().withEndpoints(access_point).build()).subscribe(topic, d->getTag()).build();
  // std::cout << "111111111111111111111111111111111111111111111111111111111111111111111111111111111111"<<std::endl;
  auto producer = ROCKETMQ_NAMESPACE::Producer::newBuilder().withConfiguration(ROCKETMQ_NAMESPACE::Configuration::newBuilder().withEndpoints(access_point).build()).build();
  
  // std::atomic_bool stopped;
  std::atomic_long send_count(0);

  // auto stats_lambda = [&]
  // {
  //   while (!stopped.load(std::memory_order_relaxed))
  //   {
  //     long cnt = send_count.load(std::memory_order_relaxed);
  //     while (send_count.compare_exchange_weak(cnt, 0))
  //     {
  //       break;
  //     }
  //     std::this_thread::sleep_for(std::chrono::seconds(1));
  //     std::cout << "QPS: " << cnt << std::endl;
  //   }
  // };

  // std::thread stats_thread(stats_lambda);

  try
  {
    for (std::size_t i = 0; i < 10; ++i)
    {
      auto message = ROCKETMQ_NAMESPACE::Message::newBuilder()
                         .withTopic(topic)
                         .withTag(d->getTag())
                         .withKeys({"Key-" + std::to_string(i)})
                         .withBody(RandomUtils::getStringByUUID())
                         .availableAfter(
                             std::chrono::system_clock::now() +
                             std::chrono::seconds(1)) // This message would be available to consumers after 10 seconds
                         .build();
      std::error_code ec;
      ROCKETMQ_NAMESPACE::SendReceipt send_receipt = producer.send(std::move(message), ec);
      std::cout << "\033[0;32m Message-ID: " << send_receipt.message_id << "\033[0m" << std::endl;
      send_count++;
    }
  }
  catch (...)
  {
    std::cerr << "Ah...No!!!" << std::endl;
  }

  // stopped.store(true, std::memory_order_relaxed);

  // if (stats_thread.joinable())
  // {
  //   stats_thread.join();
  // }

  std::this_thread::sleep_for(std::chrono::seconds(30));
  ASSERT_EQ(receive_count, send_count);
}
