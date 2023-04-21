Feature: Test the message transfer mode

  Scenario Outline:  10 normal messages are sent synchronously and are expected to be received
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic")
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And  Send "10" messages "<TransmissionMode>"
    Then Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer

    Examples:
      | TransmissionMode |
      | synchronous      |
      | asynchronous     |
