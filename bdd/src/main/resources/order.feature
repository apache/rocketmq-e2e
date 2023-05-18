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

Feature: Test the message transfer mode and topic type

  Scenario:  Send 100 messages with in 2 MessageGroup, expect consumed all messages orderly
    Given Create a "FIFO" topic:"random-topic" if not exist, a "Orderly" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And  A total of 100 messages are sent "<TransmissionMode>" to the 2 MessageGroups in turn
    Then  Check all messages that can be consumed within 60s
    And Check each MessageGroup consumes up to 50 messages separately and is consumed orderly
    And Shutdown the producer and consumer
