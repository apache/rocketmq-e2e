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

Feature: Test SimpleConsumer Initialization Parameters

  Scenario: SimpleConsumer all parameters are set properly, expect start successfully
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("10s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Check build "SimpleConsumer" successfully
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'ClientConfiguration' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ConsumerGroup("random-group"), AwaitDuration("10s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'ConsumerGroup' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), AwaitDuration("10s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'Subscription' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("10s")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'AwaitDuration' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting 'SubscriptionExpressions' empty of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("10s"), SubscriptionExpressions(NULL)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting 'AwaitDuration=0' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("0"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Consumer invoke receive(maxMessageNum:32, invisibleDuration:10s)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting 'MaxMessageNum=0' of the consumer client, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("15s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Consumer invoke receive(maxMessageNum:0, invisibleDuration:20s)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Setting 'MaxMessageNum=100000', expect start successfully
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("15s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Consumer invoke receive(maxMessageNum:1000, invisibleDuration:20s)
    Then Check "SimpleConsumer" receive messages successfully
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Error setting invisibleDuration shorter than 10000ms or longer than 43200000ms, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("<IllegalInvisibleDuration>"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Consumer invoke receive(maxMessageNum:1, invisibleDuration:20s)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

    Examples:
      | IllegalInvisibleDuration |
      | 9999ms                    |
      | 43200001ms               |

  Scenario: Setting 'AwaitDuration=10s', expect the empty pull message request to return between 10s and 20s
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "SimpleConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), AwaitDuration("10s"), SubscriptionExpressions("random-topic", "random-filterExpression")
    Then Consumer invoke receive(maxMessageNum:100000, invisibleDuration:10s)
    Then Check "SimpleConsumer" receive messages successfully
    And Check the message request return duration between 10s and 20s
    And Shutdown the producer and consumer if they are started


