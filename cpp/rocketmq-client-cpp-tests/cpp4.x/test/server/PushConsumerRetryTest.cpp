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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <cassert>

#include <gtest/gtest.h>
#include <rocketmq/SendResult.h>
#include <spdlog/logger.h>
#include <string>

#include "enums/MessageType.h"
#include "frame/BaseOperate.h"
#include "resource/Resource.h"
#include "utils/NameUtils.h"
#include "utils/RandomUtils.h"
#include "utils/VerifyUtils.h"
#include "factory/ConsumerFactory.h"
#include "factory/ProducerFactory.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;
