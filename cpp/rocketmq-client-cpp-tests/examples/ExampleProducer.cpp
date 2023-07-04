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
#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>

#include "rocketmq/Configuration.h"
#include "rocketmq/Logger.h"
#include "rocketmq/Message.h"
#include "rocketmq/Producer.h"
#include "rocketmq/SendReceipt.h"

std::string topic = "TopicTest";
std::string access_point = "127.0.0.1:9876";
int message_body_size = 4096;

int main() {

  auto configuration = ROCKETMQ_NAMESPACE::Configuration::newBuilder().withEndpoints(access_point).build();

  auto producer = ROCKETMQ_NAMESPACE::Producer::newBuilder().withConfiguration(configuration).build();

  std::string body = "123";

  try {
    std::cout<<"hh"<<std::endl;
    for (std::size_t i = 0; i < 5; ++i) {
      auto message = ROCKETMQ_NAMESPACE::Message::newBuilder()
                         .withTopic(topic)
                         .withTag("TagA")
                         .withBody(body)
                         .build();
      std::error_code ec;
      ROCKETMQ_NAMESPACE::SendReceipt send_receipt = producer.send(std::move(message), ec);
      if (ec) {
        std::cerr << "Failed to publish message to " << topic << ". Cause: " << ec.message() << std::endl;
      } else {
        std::cout << "Publish message to " << topic << " OK. Message-ID: " << send_receipt.message_id
                  << std::endl;
      }
    }
  } catch (...) {
    std::cerr << "ERROR" << std::endl;
  }
  
  std::cout<<"helloworld"<<std::endl;
  return EXIT_SUCCESS;
}
