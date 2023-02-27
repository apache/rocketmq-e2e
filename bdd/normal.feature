Feature: Test producer send  message with normal topic

  Background:
    Given username、password and endpoint

  Scenario:  send normal msgs with sync method and recv by simpleconsumer
    Given normal topic name to use
    When create normal topic
    And  build and start consumer、producer，consumer subscribe topic
    And  use producer sync send 10 msgs
    Then receive 10 msgs from server by simpleconsumer

  Scenario: send normal msgs with async method and recv by simpleconsumer
    Given normal topic name to use
    When create normal topic
    And  build and start consumer、producer，consumer subscribe topic
    And  use producer async send 10 msgs
    Then receive 10 msgs from server by simpleconsumer