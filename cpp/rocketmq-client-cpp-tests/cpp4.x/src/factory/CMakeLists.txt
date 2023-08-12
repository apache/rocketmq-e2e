set(SRC_FILES
    ConsumerFactory.cpp
    ProducerFactory.cpp
    MessageFactory.cpp
)

add_library(factory STATIC ${SRC_FILES})

target_include_directories(factory PUBLIC ${CMAKE_SOURCE_DIR}/include)