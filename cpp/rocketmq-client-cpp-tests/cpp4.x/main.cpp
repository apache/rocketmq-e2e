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
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "resource/Resource.h"
#include "utils/InitResourceUtils.h"

std::shared_ptr<spdlog::logger> multi_logger;
std::shared_ptr<Resource> resource;

int main(int argc, char* argv[]) {
    //注册控制台和文件日志输出
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs/output.log", true);
    spdlog::sinks_init_list sink_list = {console_sink, file_sink};
    multi_logger = std::make_shared<spdlog::logger>("multi", sink_list.begin(), sink_list.end());
    multi_logger->set_level(spdlog::level::trace);
    spdlog::register_logger(multi_logger);

    //读取配置文件
    resource = std::make_shared<Resource>();
    initResource(resource);
    
    //启动测试
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}