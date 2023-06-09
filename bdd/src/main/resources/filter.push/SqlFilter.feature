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

Feature: Test SQL filtering

  Scenario: Send 10 messages synchronously, then filter the messages without any attribute filtering, expect to consume 10 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression(SubExpression:"TRUE", FilterExpressionType:"SQL92"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and msgProps(regionId:"cn-hangzhou", price:"30")
    And Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Send 10 messages with messageProperty attribute 'price=10' and another 10 messages with 'price=30', then use 'price>20' to filter the messages, expect to consume 10 messages with 'price=30'
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression(SubExpression:"price>20", FilterExpressionType:"SQL92"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and msgProps(regionId:"cn-hangzhou", price:"30")
    And Send "10" messages with msgProps(price:"30") "synchronously"
    And Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and msgProps(regionId:"cn-hangzhou", price:"10")
    And Send "10" messages with msgProps(price:"10") "synchronously"
    Then Check only all messages with msgProps(price:"30") are consumed
    And Shutdown the producer and consumer if they are started

  Scenario: Send 10 messages synchronously, then use the attribute 'between{a,b}' to filter messages, expect to consume 10 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression(SubExpression:"(price BETWEEN 10 AND 100) AND regionId IS NOT NUll", FilterExpressionType:"SQL92"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and msgProps(regionId:"cn-hangzhou", price:"30")
    And Send "10" messages "synchronous"
    Then Check all messages send "successfully"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer if they are started

  Scenario: Send 10 messages synchronously, then use unknown attributes to filter messages, expect to consume 0 messages
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), Topic("random-topic"), filterExpression(SubExpression:"product = 'MQ'", FilterExpressionType:"SQL92"), and MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("random-group"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), Tag("TagA"), Key("Key"), Body("Body"), and msgProps(regionId:"cn-hangzhou", price:"30")
    And Send "10" messages "synchronous"
    And Check PushConsumer consumes 0 messages
    And Shutdown the producer and consumer if they are started



