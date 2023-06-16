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

Feature: Test some complex scenarios about pulling and ack of SimpleConsumer

  Scenario: Send a normal message, then the SimpleConsumer invokes receive(1,10s), and an ack is returned after three retries, expect retry times to be 3 and retry interval to be 10s
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "a" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"1", invisibleDuration:"10s") "synchronously"
    And SimpleConsumer returns an ack when DeliveryAttempt value equals 4
    Then Check the duration between each two retrying consumptions equals to 10s
    And Check the number of consumptions equals to 4
    And Check all messages that can be consumed and acked within 90s
    And Shutdown the producer and consumer if they are started

  Scenario: Send a normal message, then the SimpleConsumer invokes receive(1,10s), and an ack is returned within 11s section. If an INVALID_RECEIPT_HANDLE error message is displayed, then receive a new message again and return an ack successfully
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "a" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"1", invisibleDuration:"10s") "synchronously"
    And SimpleConsumer waits for 11s after receiving the messages
    Then SimpleConsumer returns an ack
    Then Check exceptions can be thrown
    And Check a new "Normal" messages are pulled "synchronously" and acked "synchronously" by SimpleConsumer within 90s "successfully"
    And Shutdown the producer and consumer if they are started

#  Scenario: Send a normal message, then the SimpleConsumer invokes receive(1,10s), then delay the invisibleTime to 20s, and consume the messages again with receive(1,30s), expect to receive and ack in 30s
#    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
#    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
#    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
#    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
#    And Send "a" messages "synchronously"
#    Then Check all messages send "successfully"
#    And SimpleConsumer invokes receive(maxMessageNum:"1", invisibleDuration:"10s", changeInvisibleDuration:"20s") "synchronously" without returning ack
#    And Check all messages that can be consumed and acked within 90s
#    And SimpleConsumer invokes receive(maxMessageNum:"1", invisibleDuration:"30s") during next consumption
#    And SimpleConsumer ack "a" messages
#    Then Check SimpleConsumer pulls the message and returns an ack within 30s but over 20s
##    用例描述里面写的是20s内，但判断的是大于20s，小于30s
#    And Shutdown the producer and consumer if they are started

#  Scenario: Send a normal message, then the SimpleConsumer invokes receive(1,10s), acks and changeInvisibleDuration, expect to change indicating illegal ReceiptHandle
#    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
#    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
#    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
#    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
#    And Send "a" messages "synchronously"
#    Then Check all messages send "successfully"
#    And SimpleConsumer invokes receive(maxMessageNum:"1", invisibleDuration:"10s") "synchronously"
#    And SimpleConsumer ack "a" messages
#    Then Set SimpleConsumer changeInvisibleDuration("10s")
#    And Check changeInvisibleDuration after the ack is failed
#    And Check the consumption is failed
#    And Shutdown the producer and consumer if they are started

  Scenario: Send 300 normal messages synchronously, then the SimpleConsumer invokes receive(50,10s), expect only up to 32 messages are consumed and acked at the same time, and no ack messages are consumed again
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "300" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"50", invisibleDuration:"10s") "synchronously"
    Then Check SimpleConsumer receives only up to 32 messages once
    And SimpleConsumer returns acks for all received messages
    And Check no acked messages that can be consumed again
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Send 20 normal messages synchronously, then SimpleConsumer invokes receive(50) in batch, and all pulled messages return ack() except the first one, expect the consumption not to be affected and the ack messages not to be consumed again
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), and Body("Body")
    And Send "20" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"50", invisibleDuration:"10s") "synchronously"
    Then SimpleConsumer returns ack for all received messages except the first one
    And Check SimpleConsumer receives only up to 32 messages once
    And Check all messages that can be consumed within 90s
    And Check only the first message is not acked
    And Shutdown the producer and consumer if they are started


