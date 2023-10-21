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
#include <iostream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "resource/Resource.h"
#include "utils/InitResourceUtils.h"

std::shared_ptr<spdlog::logger> multi_logger;
std::shared_ptr<Resource> resource;

int main(int argc, char *argv[])
{
    // Register console and file log output
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs/output.log", true);
    spdlog::sinks_init_list sink_list = {console_sink, file_sink};
    multi_logger = std::make_shared<spdlog::logger>("multi", sink_list.begin(), sink_list.end());
    multi_logger->set_level(spdlog::level::trace);
    spdlog::register_logger(multi_logger);

    // Read configuration file
    resource = std::make_shared<Resource>();
    initResource(resource);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1)
    {
        multi_logger->error("Unable to create socket");
        return 1;
    }

    // Set target IP address and port number
    std::string target_ip = resource->getNamesrv();
    std::string ip;
    int port = 9876;
    size_t colonPos = target_ip.find(":");
    if (colonPos != std::string::npos)
    {
        ip = target_ip.substr(0, colonPos);
        port = std::stoi(target_ip.substr(colonPos + 1, target_ip.size()));
    }
    else
    {
        multi_logger->error("Unable to find port number in namesrv address");
        close(sockfd);
        return 1;
    }

    // Set the target address structure
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = inet_addr(ip.c_str());
    serverAddress.sin_port = htons(port);

    // try to connect
    int connectResult = connect(sockfd, (struct sockaddr *)&serverAddress, sizeof(serverAddress));
    if (connectResult == -1)
    {
        multi_logger->error("Unable to connect to namesrv");
        close(sockfd);
        return 1;
    }

    multi_logger->info("Connect to namesrv successfully");
    close(sockfd);

    // Start-up test
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}