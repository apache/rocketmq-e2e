add_subdirectory(frame)
add_subdirectory(utils)
add_subdirectory(enums)
add_subdirectory(factory)

list(APPEND SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
set(SOURCE_FILES ${SOURCE_FILES} PARENT_SCOPE)
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