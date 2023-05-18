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

  Scenario:  Send 10 messages set delivery timestamp after 30s after the current system time. Expect to consume all 10 messages after 30s
    Given Create a "DELAY" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And Set message "DeliveryTimestamp" to 20 seconds before the current system time
    And  Send "10" messages "synchronous"
    Then Check all messages that can be consumed within 60s
    And Check the received message's "DeliveryTimestamp" property "isNotNull" and value is expected
    And Check consume all messages immediately
    And Shutdown the producer and consumer

  Scenario:  Send 10 messages set delivery timestamp 10s before the current system time. Expect timing does not take effect, all 10 messages can be consumed immediately
    Given Create a "DELAY" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And Set message "DeliveryTimestamp" to 30 seconds after the current system time
    And  Send "10" messages "synchronous"
    Then Check consume all 10 messages after 30s(±5s)
    And Check the received message's "DeliveryTimestamp" property "isNotNull" and value is expected
    And Shutdown the producer and consumer

  Scenario:  Send a message set delivery timestamp 24h+5s after the current system time. Expect send message failed
    Given Create a "DELAY" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And Set message "DeliveryTimestamp" to 86405 seconds after the current system time
    And  Send "1" messages "synchronous"
    Then Check send message failed
    And Shutdown the producer