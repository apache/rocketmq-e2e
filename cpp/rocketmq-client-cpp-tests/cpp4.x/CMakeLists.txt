# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.10)

project(rocketmq_test)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_COMPILE "g++")
set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR})

# find_library(GTEST_LIB gtest REQUIRED)
# if (NOT GTEST_LIB)
#     message(FATAL_ERROR "googletest library not found")
# endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(RocketMQ REQUIRED)


set(SOURCE_FILES "")
add_subdirectory(test)
add_subdirectory(src)

list(APPEND SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} PRIVATE
    ${GTEST_LIBRARIES}
    ${RocketMQ_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    spdlog::spdlog
    fmt::fmt
    dl
    rt
    frame
    utils
    enums
    factory
)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    /usr/include
    /usr/local/include
)

# add_executable(${PROJECT_NAME} main.cpp)

# target_link_libraries(${PROJECT_NAME} ${GTEST_LIB} ${ROCKETMQ_LIB} ${PTHREAD_LIB} ${Z_LIB} ${DL_LIB} ${RT_LIB})