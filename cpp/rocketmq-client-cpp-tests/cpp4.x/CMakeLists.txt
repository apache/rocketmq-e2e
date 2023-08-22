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
list(APPEND CMAKE_MODULE_PATH "/usr/local/lib/cmake/absl")

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(RocketMQ REQUIRED)
find_package(absl REQUIRED)


set(SOURCE_FILES "")
add_subdirectory(test)
add_subdirectory(src)

list(APPEND SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} PRIVATE 
    absl::flags_program_name
    absl::log_internal_check_op
    absl::log_globals
    absl::log_internal_globals
    absl::log_internal_message
    absl::hash
    absl::crc_cpu_detect
    absl::cord_internal
    absl::low_level_hash
    absl::flags_marshalling
    absl::random_internal_randen
    absl::flags_internal
    absl::log_internal_fnmatch
    absl::log_internal_log_sink_set
    absl::flags_usage_internal
    absl::random_internal_randen_hwaes
    absl::crc_internal
    absl::crc32c
    absl::stacktrace
    absl::flags_usage
    absl::flags_commandlineflag
    absl::flags_config
    absl::base
    absl::debugging_internal
    absl::spinlock_wait
    absl::random_internal_platform
    absl::graphcycles_internal
    absl::log_internal_proto
    absl::strings_internal
    absl::malloc_internal
    absl::random_internal_randen_slow
    absl::flags_parse
    absl::crc_cord_state
    absl::log_entry
    absl::random_internal_seed_material
    absl::flags_reflection
    absl::exponential_biased
    absl::bad_optional_access
    absl::random_seed_sequences
    absl::status
    absl::log_internal_nullguard
    absl::periodic_sampler
    absl::bad_variant_access
    absl::examine_stack
    absl::strings
    absl::log_initialize
    absl::time
    absl::log_internal_conditions
    absl::raw_hash_set
    absl::log_sink
    absl::city
    absl::throw_delegate
    absl::log_flags
    absl::demangle_internal
    absl::random_distributions
    absl::flags_commandlineflag_internal
    absl::failure_signal_handler
    absl::raw_logging_internal
    absl::cordz_functions
    absl::random_internal_pool_urbg
    absl::scoped_set_env
    absl::log_severity
    absl::cord
    absl::die_if_null
    absl::random_internal_distribution_test_util
    absl::hashtablez_sampler
    absl::string_view
    absl::log_internal_format
    absl::random_internal_randen_hwaes_impl
    absl::kernel_timeout_internal
    absl::flags_private_handle_accessor
    absl::leak_check
    absl::str_format_internal
    absl::cordz_sample_token
    absl::cordz_handle
    absl::symbolize
    absl::synchronization
    absl::int128
    absl::flags
    absl::civil_time
    absl::cordz_info
    absl::statusor
    absl::strerror
    absl::bad_any_cast_impl
    absl::time_zone
    absl::random_seed_gen_exception
    GTest::gtest
    rocketmq
    Threads::Threads
    ZLIB::ZLIB
    -ldl
    -lrt
    spdlog::spdlog
    fmt::fmt
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