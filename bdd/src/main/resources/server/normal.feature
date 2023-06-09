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

  Scenario Outline:  10 normal messages are sent synchronously and are expected to be received
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Tag("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And  Send "10" messages "<TransmissionMode>"
    Then Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

    Examples:
      | TransmissionMode |
      | synchronous      |
      | asynchronous     |
