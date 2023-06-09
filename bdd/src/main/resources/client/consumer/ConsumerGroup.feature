# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

Feature: Test System inner ConsumerGroup

  Scenario Outline: Use the built-in ConsumerGroup to consume messages and expect consume failed
    Given Create a "Concurrently" ConsumerGroup if not exist
    When Create a "SimpleConsumer", set the Endpoint("127.0.0.1:9876"), ConsumerGroup("<RmqSystemGroupConstant>"), AwaitDuration("10s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Consumer invoke receive(maxMessageNum:32, invisibleDuration:10s)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

    Examples:
      | RmqSystemGroupConstant       |
      | GROUP_DEFAULT_CONSUMER       |
      | GROUP_TOOLS_CONSUMER         |
      | GROUP_FILTERSRV_CONSUMER     |
      | GROUP_MONITOR_CONSUMER       |
      | GROUP_CLIENT_INNER_PRODUCER  |
      | GROUP_SELF_TEST_P_GROUP      |
      | GROUP_SELF_TEST_C_GROUP      |
      | GROUP_CID_ONS_HTTP_PROXY     |
      | GROUP_CID_ONSAPI_PERMISSION  |
      | GROUP_CID_ONSAPI_OWNER       |
      | GROUP_CID_ONSAPI_PULL        |
      | GROUP_CID_RMQ_SYS_TRANS      |