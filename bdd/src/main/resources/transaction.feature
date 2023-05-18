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

Feature: Test the message transfer mode

  Scenario Outline:  10 transaction messages are sent synchronously and are expected to be received
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic"), TransactionChecker:("<TransactionChecker>")
    And  Create a transaction branch
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And  Send a half message
    And  Execute transaction:"<TransactionExecutor>"
    Then Check all messages send "success"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer

    Examples:
      | TransactionChecker | TransactionExecutor |
      | COMMIT             | COMMIT              |
      | ROLLBACK           | COMMIT              |
      | UNKNOWN            | COMMIT              |

