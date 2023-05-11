Feature: Test the message transfer mode

  Scenario Outline:  10 transaction messages are sent synchronously and are expected to be received
    Given Create a "Normal" topic:"random-topic" if not exist, a "Concurrently" group:"random-group"
    When Create a PushConsumer, set the Endpoint("127.0.0.1:9876"), ConsumerGroup("random-group"), SubscriptionExpression("TagA"), Topic("random-topic"), MessageListener("default")
    And Create a Producer, set the Endpoint("127.0.0.1:9876"), RequestTimeout:("10s"), Topic("random-topic"), TransactionChecker:("<TransactionChecker>")
    And  Create a transaction branch
    Then Create a message, including the Topic("random-topic"), SubscriptionExpression("TagA"), Key("Key"), and Body("Body")
    And  Send a half message
    And  Execute transaction:"<TransactionExecutor>"
    Then Check all messages send "success"
    And Check all messages that can be consumed within 60s
    And Shutdown the producer and consumer

    Examples:
      | TransactionChecker | TransactionExecutor |
      | COMMIT             | COMMIT              |
      | ROLLBACK           | COMMIT              |
      | UNKNOWN            | COMMIT              |

