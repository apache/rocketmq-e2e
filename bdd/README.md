# Background

The goal of the RocketMQ5.0 client SDK is to provide the implementation of all mainstream programming languages,it is inevitable to consider how to solve quality problem of each programming language client.
Although the client implementation of each programming language are different, the user behavior is consistent. Taking the RocketMQ5.0 Java client as an example, it can be seen from the interface exposed to the user that the classes and methods involved in the actual use for the user are limited.
Based on the following two foundations, a language-independent E2E test co-construction method for client of each programming language is proposed:

- The results of user behavior are consistent in each language. E2E test scenarios can cover 99% of user scenarios or behaviors through a reasonable combination of user behaviors on the exposed limited interfaces.
- In the BDD given-when-then way, which is used as a unified description and definition of E2E test scenarios for each programming language client. It is possible to achieve unified semantics of E2E tests under different programming language SDKs. Realize the decoupling of E2E actual coding and definition.

## BDD example
```
exmaples:

Feature: Test producer send  message with normal topic

  Background:
    Given Instance username、password and endpint

  Scenario:  send normal msgs sync
    Given normal topic name to use
    When create normal topic
    And  create  and start producer
    And  use producer send 10 msgs
    Then receive 10 success response from server

  Scenario:  send normal msgs async
    Given normal topic name to use
    When create normal topic
    And  create and start producer
    And  use producer async send 10 msgs
    Then receive 10 success callback response from server
```
## Code implementation

At present, the Java and Go E2E test scenarios have been open sourced, welcome to co-construction. Use the test frameworks of each programming language to establish E2E test frameworks and test scenarios in each programming language, and improve the client quality of RocketMQ5.0 together.
