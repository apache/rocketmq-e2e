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

Feature: Test retry of PushConsumer

  Scenario: The normal message is sent, and after the PushConsumer partial retry, the retry message is expected to be consumed
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-FilterExpression"), ConsumptionThreadCount(20)
    And Set PushConsumer Listener, “half” of the number of pre-sent messages are consumed successful and return SUCCESS, another "half" are consumed failed and return FAILURE
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And  Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    Then Check NoRetryMessages equals to RetryMessages and equals to "half" of the number of pre-sent messages
    And Check all messages are contained in retryMessages or noRetryMessages
    And Shutdown the producer and consumer if they are started

  Scenario: The send order message, after the PushConsumer partial retry, is expected to consume the retry message, and the message consumption order and send order
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-FilterExpression"), ConsumptionThreadCount(20), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and MessageGroup("group")
    And  Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Wait until all the messages that can be consumed
    And Check the order of received messages consistent with the order of pre-sent messages
    And Shutdown the producer and consumer if they are started

  Scenario: Send sequential messages, using three Shardingkeys, after partial retries, expect to consume retry messages, and the order of message consumption and the order of message delivery
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a "PushConsumer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpressions("random-topic", "random-FilterExpression"), ConsumptionThreadCount(20), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and MessageGroup("group")
    And  Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Wait until all the messages that can be consumed
    Then Separate the messages into 3 ShardingKeyGroups according to messageGroup
    And Check the message order in each ShardingKeyGroup
    And Shutdown the producer and consumer if they are started

