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

Feature: Test user property

  Scenario: Message user property beyond limit 128 ,expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), and userProperty("size:129")
    And  Send "1" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: The number of message user properties equals limit 128, expect send success
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), and userProperty("size:128")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Setting illegal Message user property, expect send success
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), Body("random-body"), Key("<KeyContent>"), and Value("<ValueContent>")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started
    Examples:
      | KeyContent | ValueContent |
      | size:8kB   | size:8kB     |
      |   中文      |   中文        |
      |    _       |    _         |
      |    %       |    %         |
      |    。      |    。         |
      |    ｜      |    ｜         |
      |    &&      |    &&        |
      |    🏷      |    🏷        |


  Scenario Outline: Setting illegal user properties, expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), Body("random-body"), Key("<KeyContent>"), and Value("<ValueContent>")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

    Examples:
      | KeyContent | ValueContent |
      | size:8kB   | size:8kB+1   |
      | \u0000     | value        |
      | UNIQ_KEY   | value        |

  Scenario: Message user property ,key and tag beyond 16KB ,expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), Tag("size:4kB"), Key("size:4kB"), Value("size:4kB"), Body("size:4M"), msgKey("size:4kB+1")
    And  Send "1" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Message user property ,key and tag equals 16KB, expect send success
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), Tag("size:4kB"), Key("size:4kB"), Value("size:4kB"), Body("size:4M"), msgKey("size:4kB")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started

  Scenario: Message user property ,key and tag equals 64B, expect send success
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("Normal random-topic"), Tag("size:64B"), Key("size:64B"), Value("size:64B"), Body("size:64B"), msgKey("size:64B")
    And  Send "1" messages "synchronous"
    Then Check there no fail message occurs
    And Shutdown the producer and consumer if they are started







