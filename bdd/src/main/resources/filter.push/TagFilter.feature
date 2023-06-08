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

Feature: Test Tag filtering

  Scenario: Use Tag='TagA' to send 10 messages, then use 'TagA||TagB' to filter messages, expect to consume 10 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("TagA||TagB"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='TagA' to send 10 messages, and use Tag='TagB' to send another 10 messages, then use 'TagA||TagB' to filter messages, expect to consume 20 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("TagA||TagB"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    Then Send "10" messages with Tag("TagA") "synchronous"
    And Create a message, including the Topic("random-topic"), Tag("TagB"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagB") "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='TagA' to send 10 messages, and use Tag='TagB' to send another 10 messages, then use '*' to filter messages, expect to consume 20 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("*"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagA") "synchronous"
    And Create a message, including the Topic("random-topic"), Tag("TagB"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagB") "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='TagA' to send 10 messages, then use Tag='TagB' to filter messages, expect to consume 0 message
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("TagB"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    Then Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check PushConsumer consumes 0 messages
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='TagA' to send 10 messages, then use Tag='TagA' to filter messages, expect to consume 10 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("TagA"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    Then Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use a very long Tag to send 10 messages, expect to consume 10 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("Tag"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagSize:10kB"), Key("Key"), and Body("Body")
    Then Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='TagA' to send 10 messages, and use Tag='TagB' to send another 10 messages, then use a tag which adds space on both sides of sent tags to filter messages, expect to consume 20 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression(" TagA||TagB "), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagA") "synchronous"
    And Create a message, including the Topic("random-topic"), Tag("TagB"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagB") "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='|@' to send 10 messages, expect to start failed (tags are not allowed to contain '|')
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("|@"), Key("Key"), and Body("Body")
    Then Send "10" messages "synchronous"
    And Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='*' to send 10 messages, then use '*' to filter messages, expect to consume 10 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("*"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("*"), Key("Key"), and Body("Body")
    Then Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='TagA' to send 10 messages, and use Tag='TagB' to send another 10 messages, then use use || separators between two tags to filter messages, expect to consume 20 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("TagA||||TagB"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagA") "synchronous"
    And Create a message, including the Topic("random-topic"), Tag("TagB"), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("TagB") "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag="" to send 10 messages, and use Tag=" " to send another 10 messages, expect to start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag(""), Key("Key"), and Body("Body")
    And Send "10" messages with Tag("") "synchronous"
    And Create a message, including the Topic("random-topic"), Tag(" "), Key("Key"), and Body("Body")
    And Send "10" messages with Tag(" ") "synchronous"
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Use two Tags='BB' and 'Aa' with the same hash value to send 10 messages, respectively, then use 'BB' to filter messages, expect to consume 10 messages with Tag='BB'
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("BB"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("BB"), Key("Key"), and Body("Body")
    Then Check all messages that can be consumed within 60s
    And Create a message, including the Topic("random-topic"), Tag("Aa"), Key("Key"), and Body("Body")
    And Check PushConsumer consumes 0 messages
    And Shutdown the producer and consumer if they are started

  Scenario: Use Tag='BB' to send 10 messages, and use Tag='bb' to send another 10 messages, expect to consume messages with tag='BB'
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression("BB"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a message, including the Topic("random-topic"), Tag("BB"), Key("Key"), and Body("Body")
    Then Check all messages that can be consumed within 60s
    And Create a message, including the Topic("random-topic"), Tag("bb"), Key("Key"), and Body("Body")
    And Check PushConsumer consumes 0 messages
    And Shutdown the producer and consumer if they are started

