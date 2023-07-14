set(SRC_FILES
    BaseOperate.cpp
)

add_library(frame STATIC ${SRC_FILES})
target_link_libraries(frame fmt::fmt)
target_include_directories(frame PUBLIC ${CMAKE_SOURCE_DIR}/include)