Feature: Test the message transfer mode and topic type

  Scenario:  Send 10 messages set delivery timestamp after 30s after the current system time. Expect to consume all 10 messages after 30s
    Given Create a "DELAY" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And Set message "DeliveryTimestamp" to 20 seconds before the current system time
    And  Send "10" messages "synchronous"
    Then Check all messages that can be consumed within 60s
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
    And Shutdown the producer and consumer

  Scenario:  Send a message set delivery timestamp 24h+5s after the current system time. Expect send message failed
    Given Create a "DELAY" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And Set message "DeliveryTimestamp" to 86405 seconds after the current system time
    And  Send "1" messages "synchronous"
    Then Check send message failed
    And Shutdown the producer