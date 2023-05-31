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

Feature: Test normal message size

  Scenario Outline: Send normal/transaction messages synchronously with the body size of 4M+1, expect send failed
    Given Create a "<MessageType>" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("<MessageTopic>"), Tag("random-tag"), and Body("size:4M+1")
    And  Send "1" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started


    Examples:
      | MessageType   |     MessageTopic        |
      |   Normal      |   Normal random-topic   |
      | transaction   | transaction random-topic|

  Scenario: Send delay messages synchronously with the body size of 4M+1, expect send failed
    Given Create a "delay" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("delay random-topic"), Tag("random-tag"), Body("size:4M+1"), deliveryTimestamp("10L")
    And  Send "1" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Send delay messages synchronously with the body size of 4M+1, expect send failed
    Given Create a "FIFO" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("FIFO random-topic"), Tag("random-tag"), Body("size:4M+1"), messageGroup("a")
    And  Send "1" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Send normal/transaction messages synchronously with the body size of 4M, expect send failed
    Given Create a "<MessageType>" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("<MessageTopic>"), Tag("random-tag"), and Body("size:4M")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

    Examples:
      | MessageType   |     MessageTopic        |
      |   Normal      |   Normal random-topic   |
      | transaction   | transaction random-topic|

  Scenario: Send delay messages synchronously with the body size of 4M, expect send success
    Given Create a "delay" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("delay random-topic"), Tag("random-tag"), Body("size:4M"), deliveryTimestamp("10L")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

  Scenario: Send FIFO messages synchronously with the body size of 4M, expect send success
    Given Create a "FIFO" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("FIFO random-topic"), Tag("random-tag"), Body("size:4M"), messageGroup("a")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

  Scenario: Send normal messages synchronously with the body size of 4M and the user property size of 16KB, expect send success
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), Body("size:4M"), Key("size:8kB"), and Value("size:8kB")
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

  Scenario: Send FIFO messages synchronously with the body size of 4M and the user property size of 16KB, expect send success
    Given Create a "FIFO" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("FIFO random-topic"), Body("size:4M"), messageGroup("a"), Key("size:8kB"), and Value("size:8kB")
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started










