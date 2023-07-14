set(SRC_FILES
    MessageType.cpp
)

add_library(enums STATIC ${SRC_FILES})
target_include_directories(enums PUBLIC ${CMAKE_SOURCE_DIR}/include)