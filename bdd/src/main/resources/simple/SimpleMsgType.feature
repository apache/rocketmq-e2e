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

Feature: Test SimpleConsumer pulls and ack order/delay/transaction messages synchronously properly

  Scenario: Send 20 order messages synchronously, expect SimpleConsumer to receive() and ack() messages properly and the order to be maintained
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create "a" messages, including the Topic("Topic"), Tag("Tag"), Key("Key"), MessageGroup("Group"), and Body("Body")
    And Send "20" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"1", invisibleDuration:"10s") "synchronously"
    Then SimpleConsumer returns acks "synchronously"
    Then Check all "FIFO" messages are received and acked within 90s "successfully"
    And Shutdown the producer and consumer if they are started

  Scenario: Send 10 delay messages synchronously, expect SimpleConsumer to receive() and ack() messages properly
    Given Create a "Delay" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Body("Body"), deliveryTimestamp("10s")
    And Send "10" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"1", invisibleDuration:"10s") "synchronously"
    Then SimpleConsumer returns acks "synchronously"
    Then Check all "Delay" messages are received and acked within 90s "successfully"
    And Shutdown the producer and consumer if they are started

  Scenario: Send 10 transaction messages synchronously, expect SimpleConsumer to receive() and ack() messages properly
    Given Create a "Transaction" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "10" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"1", invisibleDuration:"10s") "synchronously"
    Then SimpleConsumer returns acks "synchronously"
    Then Check all "Transaction" messages are received and acked within 90s "successfully"
    And Shutdown the producer and consumer if they are started
