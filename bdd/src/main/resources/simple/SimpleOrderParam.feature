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

Feature: Test SimpleConsumer pulls, ack and retry order messages

  Scenario: Send 20 order messages synchronously with the same MessageGroup, then SimpleConsumer consumes messages orderly with receive() but not ack() messages, expect messages to be stuck at the first message
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create "20" messages, including the Topic("random-topic"), Tag("TagA"), Key("Key"), MessageGroup("group1"), and Body("1-20")
    And Send "20" messages "synchronously"
    Then Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"3", invisibleDuration:"10s") "synchronously"
    Then SimpleConsumer returns acks for all received messages except the first one with Body("1")
    Then Check only the first "1" messages with Body("1") that can be received
    And Shutdown the producer and consumer if they are started

#  Scenario: Send 20 order messages synchronously with eight different MessageGroups, then SimpleConsumer consumes messages orderly with receive() but not ack() messages, expect the order to be maintained
#    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
#    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
#    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
#    And Create "20" messages, including the Topic("random-topic"), Tag("TagA"), Key("Key"), MessageGroup("8 different groups"), and Body("Body")
#    And Send "20" messages "synchronously"
#    Then Check all messages send "successfully"
#    And SimpleConsumer invokes receive(1) "synchronously"
#    Then Check only the first message that can be pulled by SimpleConsumer
#    And Shutdown the producer and consumer if they are started


  Scenario: Send 20 order messages synchronously, then SimpleConsumer invokes receive(3) in batch, and all pulled messages return ack() except the first one, expect the order to be maintained and the messages to be consumed again after a certain time
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
    And Create "20" messages, including the Topic("random-topic"), Tag("TagA"), Key("Key"), MessageGroup("group1"), and Body("1-20")
    And Send "20" messages "synchronously"
    And Check all messages send "successfully"
    And SimpleConsumer invokes the method receive(maxMessageNum:"3", invisibleDuration:"10s") "synchronously"
    Then SimpleConsumer returns acks for all received messages except the first one with Body("1")
    And Check only the first "3" messages with Body("1-3") that can be received
    And Check all received messages that can be consumed again "successfully"
#    第一条没有返回ack，因此会被重新拉取，由于发送的是顺序消息，所以后面的消息也会被重新拉取
    And Shutdown the producer and consumer if they are started
#
#  Scenario: Send 20 order messages synchronously, then SimpleConsumer invokes receive(3) in batch, only ack() the first one of pulled messages, expect the order to be maintained and other messages to be consumed again after a certain time
#    Given Create a "FIFO" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
#    When Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
#    And Create a SimpleConsumer, set the Endpoint("127.0.0.1:9876"), Topic("random-topic"), ConsumerGroup("group"), FilterExpressions("tag"), Duration("10s")
#    And Create "20" messages, including the Topic("random-topic"), Tag("TagA"), Key("Key"), MessageGroup("group1"), and Body("Body")
#    And Send "20" messages "synchronously"
#    And Check all messages send "successfully"
#    Then SimpleConsumer invokes receive(3) "synchronously" and returns acks "synchronously" for "the first one" pulled messages
#    And Check the pulled messages that can be retried except the first one
#    And Check the number of retried messages equals to 2
##    如果一起拉取三条，只有第一条返回ack，剩余两条会被重试，那么viewList.stream().filter(msg -> msg.getDeliveryAttempt() == 1).count() 应该是2，原来用例是等于1，而且getDeliveryAttenmpt是从0开始计数的
#    And Shutdown the producer and consumer if they are started
