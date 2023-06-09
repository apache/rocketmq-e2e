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

Feature: Test Producer Initialization Parameters

  Scenario: Producer is normally set, expected start successfully
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), Topics("random-topic"), and MaxAttempts(3)
    Then Check build "Producer" successfully
    And Shutdown the producer and consumer if they are started

  Scenario Outline: Error setting the ClientConfiguration(including Endpoint and CredentialProvider) of Producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the Topics("random-topic")
    And Set "Producer" ClientConfiguration(Endpoint:"<Endpoint>", AccessKey:"<AccessKey>", SecretKey:"<SecretKey>")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

    Examples:
      | Endpoint               | AccessKey | SecretKey |
      | 127.0.0.1:9876         | errorAk   | accountSK |
      | 127.0.0.1:9876         | accountAk | errorSK   |
      | https://www.aliyun.com | accountAk | accountSK |

  Scenario: Without setting the 'AccessKey' of Producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the Topics("random-topic")
    And Set "Producer" ClientConfiguration(Endpoint:"127.0.0.1:9876", AccessKey:NULL, SecretKey:"accountSK")
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting the 'SecretKey' of Producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the Topics("random-topic")
    And Set "Producer" ClientConfiguration(Endpoint:"127.0.0.1:9876", AccessKey:"accountAK", SecretKey:NULL)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting the properties(including Endpoints, AccessKey, SecretKey) in 'ClientConfiguration' of Producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(NULL), Topics("random-topic"), and MaxAttempts(3)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Set the 'MaxAttempts' of Producer to 0, expect start successfully
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), Topics("random-topic"), and MaxAttempts(0)
    Then Check build "Producer" successfully
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting the 'MaxAttempts' of Producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), Topics("random-topic"), and MaxAttempts(-1)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting the 'Topic' of the Producer to non-existent topic, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), Topics("topicNotExist"), and MaxAttempts(3)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Error setting the same 'Topics' of the producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), Topics("topic1", "topic1"), and MaxAttempts(3)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Set more topics of the Producer, one of which does not exist, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), Topics("random-topic", "topicNotExist"), and MaxAttempts(3)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'ClientConfiguration' of Producer, expect start failed
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the Topics("random-topic"), and MaxAttempts(3)
    Then Check exceptions can be thrown
    And Shutdown the producer and consumer if they are started

  Scenario: Without setting 'Topics' of Producer, expect start successfully
    Given Create a "Normal" topic:"random-topic" if not exist
    When Create a "Producer", set the ClientConfiguration(Endpoint:"127.0.0.1:9876"), and MaxAttempts(3)
    Then Check build "Producer" successfully
    And Shutdown the producer and consumer if they are started
