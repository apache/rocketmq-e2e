# 背景

RocketMQ5.0 客户端SDK的目标是提供各种主流编程语言的原生实现，不可避免的要考虑如何解决各个语言客户端的代码质量问题。尽管各编程语言的客户端实现形式不同，但是用户行为表现是一致的。以RocketMQ5.0 Java客户端为例，通过暴露给用户的接口可以看到，用户实际使用中涉及的class及method有限。基于以下两点，提出了一个语言无关的各编程语言客户端E2E测试方法：

- 用户行为在各语言下结果一致，E2E测试场景通过对暴露的有限的接口进行用户行为的合理组合，可以做到99%的用户使用场景或行为覆盖。
- 以BDD given-when-then 方式 作为各个语言客户端的E2E测试场景的统一描述定义。可以做到E2E测试在不同编程语言SDK下统一语义。实现E2E实际编码与定义解耦。

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
## 代码实现

目前已开源java、go E2E测试场景，欢迎大家积极共建。利用各语言原生的测试框架建立各语言的E2E测试框架及测试场景，一起提高RocketMQ5.0的客户端质量。
