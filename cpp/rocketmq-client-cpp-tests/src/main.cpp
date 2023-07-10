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
#include <atomic>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <system_error>

#include "rocketmq/CredentialsProvider.h"
#include "rocketmq/Logger.h"
#include "rocketmq/Message.h"
#include "rocketmq/Producer.h"

using namespace ROCKETMQ_NAMESPACE;

const std::string& alphaNumeric() {
  static std::string alpha_numeric("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
  return alpha_numeric;
}

std::string randomString(std::string::size_type len) {
  std::string result;
  result.reserve(len);
  std::random_device rd;
  std::mt19937 generator(rd());
  std::string source(alphaNumeric());
  std::string::size_type generated = 0;
  while (generated < len) {
    std::shuffle(source.begin(), source.end(), generator);
    std::string::size_type delta = std::min({len - generated, source.length()});
    result.append(source.substr(0, delta));
    generated += delta;
  }
  return result;
}

std::string topic = "standard_topic_sample";
std::string access_point= "121.196.167.124:8081";
int message_body_size=4096;
int total= 256;
std::string access_key="";
std::string access_secret="";

int main(int argc, char* argv[]) {

  auto& logger = getLogger();
  logger.setConsoleLevel(Level::Info);
  logger.setLevel(Level::Info);
  logger.init();

  // Access Key/Secret pair may be acquired from management console
  CredentialsProviderPtr credentials_provider;
  if (!access_key.empty() && !access_secret.empty()) {
    credentials_provider = std::make_shared<StaticCredentialsProvider>(access_key, access_secret);
  }

  // In most case, you don't need to create too many producers, singletion pattern is recommended.
  auto producer = Producer::newBuilder()
                      .withConfiguration(Configuration::newBuilder()
                                             .withEndpoints(access_point)
                                             .withCredentialsProvider(credentials_provider)
                                             .build())
                      .build();

  std::atomic_bool stopped;
  std::atomic_long count(0);

  auto stats_lambda = [&] {
    while (!stopped.load(std::memory_order_relaxed)) {
      long cnt = count.load(std::memory_order_relaxed);
      while (count.compare_exchange_weak(cnt, 0)) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      std::cout << "QPS: " << cnt << std::endl;
    }
  };

  std::thread stats_thread(stats_lambda);

  std::string body = randomString(message_body_size);

  try {
    for (std::size_t i = 0; i < total; ++i) {
      auto message = Message::newBuilder()
                         .withTopic(topic)
                         .withTag("TagA")
                         .withKeys({"Key-" + std::to_string(i)})
                         .withBody(body)
                         .build();
      std::error_code ec;
      SendReceipt send_receipt = producer.send(std::move(message), ec);
      if (ec) {
        std::cerr << "Failed to publish message to " << topic << ". Cause: " << ec.message() << std::endl;
      } else {
        std::cout << "Publish message to " << topic << " OK. Message-ID: " << send_receipt.message_id
                  << std::endl;
        count++;
      }
    }
  } catch (...) {
    std::cerr << "Ah...No!!!" << std::endl;
  }
  stopped.store(true, std::memory_order_relaxed);
  if (stats_thread.joinable()) {
    stats_thread.join();
  }

  return EXIT_SUCCESS;
}
