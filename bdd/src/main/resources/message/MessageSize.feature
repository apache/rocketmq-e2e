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

Feature: Test message size

  Scenario Outline: Send normal/transaction messages with the body size of 4M+1, expect send failed
    Given Create a "<MessageType>" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), and Body("size:4M+1")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

    Examples:
      | MessageType   |
      |   Normal      |
      | Transaction   |

  Scenario: Send delay messages with the body size of 4M+1, expect send failed
    Given Create a "delay" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Body("size:4M+1"), deliveryTimestamp("10L")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Send FIFO messages with the body size of 4M+1, expect send failed
    Given Create a "FIFO" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Body("size:4M+1"), messageGroup("a")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Send normal/transaction messages with the body size of 4M, expect send and consume success
    Given Create a "<MessageType>" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), and Body("size:4M")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

    Examples:
      | MessageType   |
      |   Normal      |
      | Transaction   |

  Scenario: Send delay messages with the body size of 4M, expect send and consume success
    Given Create a "delay" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Body("size:4M"), deliveryTimestamp("10s")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Send FIFO messages with the body size of 4M, expect send and consume success
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Body("size:4M"), messageGroup("a")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Send normal messages with the body size of 4M and the user property size of 16KB, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Body("size:4M"), and userProperty("size:16kB")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Send FIFO messages with the body size of 4M and the user property size of 16KB, expect send and consume success
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Body("size:4M"), messageGroup("a"), and userProperty("size:16kB")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started










