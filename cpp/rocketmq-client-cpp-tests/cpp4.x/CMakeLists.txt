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

include_directories(${PROJECT_SOURCE_DIR}/include)

set(SOURCE_FILES "")
add_subdirectory(test)
add_subdirectory(src)

list(APPEND SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} PRIVATE 
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
)

# add_executable(${PROJECT_NAME} main.cpp)

# target_link_libraries(${PROJECT_NAME} ${GTEST_LIB} ${ROCKETMQ_LIB} ${PTHREAD_LIB} ${Z_LIB} ${DL_LIB} ${RT_LIB})