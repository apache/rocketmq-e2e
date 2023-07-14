set(SRC_FILES
    MQAdminUtils.cpp
    NameUtils.cpp
    RandomUtils.cpp
)

add_library(utils STATIC ${SRC_FILES})

target_include_directories(utils PUBLIC ${CMAKE_SOURCE_DIR}/include)