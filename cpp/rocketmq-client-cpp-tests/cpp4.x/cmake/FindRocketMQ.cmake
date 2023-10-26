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
set(ROCKETMQ_CPP_LIB_PATH $ENV{ROCKETMQ_CPP_LIB})
set(ROCKETMQ_CPP_INC_PATH $ENV{ROCKETMQ_CPP_INC})

find_path(ROCKETMQ_INCLUDE_DIR
  NAMES
    rocketmq/DefaultMQProducer.h
  PATHS
    /usr/local/include
    /usr/include
    ${ROCKETMQ_CPP_INC_PATH}
)

find_library(ROCKETMQ_LIBRARY
  NAMES
    rocketmq
  PATHS
    /usr/local/lib
    /usr/lib
    ${ROCKETMQ_CPP_LIB_PATH}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocketMQ
  REQUIRED_VARS
    ROCKETMQ_LIBRARY
    ROCKETMQ_INCLUDE_DIR
)

if(RocketMQ_FOUND)
  set(RocketMQ_LIBRARIES ${ROCKETMQ_LIBRARY})
  set(RocketMQ_INCLUDE_DIRS ${ROCKETMQ_INCLUDE_DIR})
  mark_as_advanced(RocketMQ_INCLUDE_DIRS RocketMQ_LIBRARIES)
endif()