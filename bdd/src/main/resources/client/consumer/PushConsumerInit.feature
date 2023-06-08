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

Feature: Test PushConsumer Initialization Parameters

  Scenario: PushConsumer all parameters are set properly, expect start successfully
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    Given Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-filterExpression"), ConsumptionThreadCount(20), MaxCacheMessageCount(1000), MaxCacheMessageSizeInBytes(4M), MessageListener("default")
    Then Check build "PushConsumer" successfully
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Error setting the ClientConfiguration(including Endpoint and CredentialProvider), expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-filterExpression"), MessageListener("default")
    And Set "PushConsumer" ClientConfiguration(Endpoint:"<Endpoint>", AccessKey:"<AccessKey>", SecretKey:"<SecretKey>")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

    Examples:
      | Endpoint               | AccessKey | SecretKey |
      | 127.0.0.1:9876         | errorAk   | accountSK |
      | 127.0.0.1:9876         | accountAk | errorSK   |
      | https://www.aliyun.com | accountAk | accountSK |

  Scenario: Error setting the 'Topic' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("topicNotExist", "random-filterExpression"), MessageListener("default")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'ConsumerGroup' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), SubscriptionExpressions("random-topic", "random-filterExpression"), MessageListener("default")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'SubscriptionExpressions' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), MessageListener("default")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting an empty 'SubscriptionExpressions' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions(NULL), MessageListener("default")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'ClientConfiguration' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), SubscriptionExpressions("random-topic", "random-filterExpression"), MessageListener("default")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'MessageListener' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started


