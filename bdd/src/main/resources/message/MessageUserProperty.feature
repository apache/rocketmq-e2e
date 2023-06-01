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

Feature: Test message property

  Scenario: Message user property beyond limit 128 ,expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Body("random-body"), and messageProperty("random-messageProperty")
    And Set message "messageProperty" "129" times
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: The number of message user properties equals limit 128, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Body("random-body"), and messageProperty("random-messageProperty")
    And Set message "messageProperty" "128" times
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Message user property equals 16KB, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Body("random-body"), and messageProperty("size:16kB")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started


  Scenario: Message user property beyond 16KB, expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Body("random-body"), and messageProperty("size:16kB+1")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Message user property contains invisible character \u0000 / use SystemKey UNIQ_KEY ,expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), and messageProperty("<KeyContent>", "<ValueContent>")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

    Examples:
      | KeyContent | ValueContent |
      | \u0000     | value        |
      | UNIQ_KEY   | value        |

  Scenario: Message user property ,key and tag beyond 16KB ,expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("size:4kB"), Key("size:4kB"), Value("size:4kB"), Body("size:4M"), msgKey("size:4kB+1")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Message user property ,key and tag equals 16KB, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("size:4kB"), Key("size:4kB"), Value("size:4kB"), Body("size:4M"), msgKey("size:4kB")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Message user property ,key and tag equals 64B, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("size:64B"), Key("size:64B"), Value("size:64B"), Body("size:64B"), msgKey("size:64B")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Message user property is the visible character, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Key("中文"), Value("中文"), Body("random-body"), msgKey("random-msgkey")
    And Set messageProperty "Key" to "_" and "Value" to "_"
    And Set messageProperty "Key" to "%" and "Value" to "%"
    And Set messageProperty "Key" to "。" and "Value" to "。"
    And Set messageProperty "Key" to "｜" and "Value" to "｜"
    And Set messageProperty "Key" to "&&" and "Value" to "&&"
    And Set messageProperty "Key" to "🏷" and "Value" to "🏷"
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started







