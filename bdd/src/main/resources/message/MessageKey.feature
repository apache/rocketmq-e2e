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

Feature: Test message key

  Scenario: Message Key beyond 16KB, expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Key("size:16kB+1"), and Body("random-body")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Message Key beyond 16KB, expect throw exception
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Key("\u0000"), and Body("random-body")
    And  Send "a" messages "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Message Key equals 16KB, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Key("size:16KB"), and Body("random-body")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Message key contains Chinese, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Key("中文字符"), and Body("random-body")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: The message contains multiple keys, expect send and consume success
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a message, including the Topic("random-topic"), Tag("random-tag"), Key("random-key1", "random-key2"), and Body("random-body")
    And  Send "a" messages "synchronous"
    Then  Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started


