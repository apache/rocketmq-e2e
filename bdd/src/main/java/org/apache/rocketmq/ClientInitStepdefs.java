/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class ClientInitStepdefs {

    @And("Create a Producer, set the <NameServer>, <RequestTimeout>")
    public void createAProducerSetTheNameServerRequestTimeout() {
    }

    @And("Create a Producer, set the {string}, {string}")
    public void createAProducerSetThe(String nameserver, String requestTimeout) {
    }

    @Then("Create a message, including the {string}, {string}, {string}, and {string}")
    public void createAMessageIncludingTheAnd(String arg0, String arg1, String arg2, String arg3) {
    }

    @When("Create a PushConsumer, set the {string}, {string}, {string}:{string}, {string}")
    public void createAPushConsumerSetThe(String arg0, String arg1, String arg2, String arg3, String arg4) {

    }

    @And("Send {string} messages {string}")
    public void sendMessages(String arg0, String arg1) {
        

    }

    @Then("Check all messages that can be consumed within {int}s")
    public void checkAllMessagesThatCanBeConsumedWithinS(int arg0) {

    }

    @And("A total of {int} messages are sent {string} to the {int} MessageGroups in turn")
    public void aTotalOfMessagesAreSentToTheMessageGroupsInTurn(int arg0, String arg1, int arg2) {

    }

    @Given("Create a {string} topic:{string} if not exist, a {string} group:{string}")
    public void createATopicIfNotExistAGroup(String arg0, String arg1, String arg2, String arg3) {

    }

    @And("Create a Producer, set the {string}, {string}, {string}")
    public void createAProducerSetThe(String arg0, String arg1, String arg2) {

    }

    @And("Create a Producer, set the {string}, {string}, {string}, {string}")
    public void createAProducerSetThe(String arg0, String arg1, String arg2, String arg3) {


    }

    @And("Execute transaction:{string}")
    public void executeTransaction(String arg0) {

    }

    @Then("Check all messages send {string}")
    public void checkAllMessagesSend(String arg0) {

    }

    @When("Create a PushConsumer, set the Endpoint\\({string}), ConsumerGroup\\({string}), Tag\\({string}), Topic\\({string}), MessageListener\\({string})")
    public void createAPushConsumerSetTheEndpointConsumerGroupTagTopicMessageListener(String arg0, String arg1, String arg2, String arg3, String arg4) {
        

    }

    @And("Create a Producer, set the Endpoint\\({string}), RequestTimeout:\\({string}), Topic\\({string}), TransactionChecker:\\({string})")
    public void createAProducerSetTheEndpointRequestTimeoutTopicTransactionChecker(String arg0, String arg1, String arg2, String arg3) {

    }

    @Then("Create a message, including the Topic\\({string}), Tag\\({string}), Key\\({string}), and Body\\({string})")
    public void createAMessageIncludingTheTopicTagKeyAndBody(String arg0, String arg1, String arg2, String arg3) {
        

    }

    @And("Create a Producer, set the Endpoint\\({string}), RequestTimeout:\\({string}), Topic\\({string})")
    public void createAProducerSetTheEndpointRequestTimeoutTopic(String arg0, String arg1, String arg2) {
        

    }

    @Then("Check send message failed")
    public void checkSendMessageFailed() {

    }

    @And("Check each MessageGroup consumes up to {int} messages separately and is consumed orderly")
    public void checkEachMessageGroupConsumesUpToMessagesSeparatelyAndIsConsumedOrderly(int arg0) {
    }
    

    @And("Create a SimpleConsumer, set the Endpoint\\({string}), ConsumerGroup\\({string}), SubscriptionExpression\\({string}), Topic\\({string}), Duration\\({string})")
    public void createASimpleConsumerSetTheEndpointConsumerGroupSubscriptionExpressionTopicDuration(String arg0, String arg1, String arg2, String arg3, String arg4) {
    }

    @And("Check the subscribed message body is equal to {string}")
    public void checkTheSubscribedMessageBodyEqualsTo(String arg0) {
    }

    @And("Create a message, including the Topic\\({string}), Tag\\({string}), Key\\({string}), and Body\\(null)")
    public void createAMessageIncludingTheTopicTagKeyAndBodyNull(String arg0, String arg1, String arg2) {
    }


    @And("Create a message, including the Topic\\(null), Tag\\({string}), Key\\({string}), and Body\\({string})")
    public void createAMessageIncludingTheTopicNullTagKeyAndBody(String arg0, String arg1, String arg2) {
        
    }

    @And("Create a message, including the Topic\\({string}), Tag\\(null), Key\\({string}), and Body\\({string})")
    public void createAMessageIncludingTheTopicTagNullKeyAndBody(String arg0, String arg1, String arg2) {

    }

    @And("Create a message, including the Topic\\({string}), SubscriptionExpression\\({string}), Key\\(RandomStringUtils.randomAlphabetic\\({int} * {int} + {int})), and Body\\({string})")
    public void createAMessageIncludingTheTopicSubscriptionExpressionKeyRandomStringUtilsRandomAlphabeticAndBody(String arg0, String arg1, int arg2, int arg3, int arg4, String arg5) {

    }

    @Then("Check exceptions can be thrown")
    public void checkBuildMethodThatCanThrowExceptions() {
    }

    @Given("Create a {string} topic:{string} if not exist")
    public void createATopicIfNotExist(String arg0, String arg1) {


    }

    @And("Shutdown the producer and consumer if they are started")
    public void shutdownTheProducerAndConsumerIfTheyAreStarted() {
    }

    @And("Create a message, including the Topic\\({string}), Tag\\({string}), Key\\({string}, {string}), and Body\\({string})")
    public void createAMessageIncludingTheTopicTagKeyAndBody(String arg0, String arg1, String arg2, String arg3, String arg4) {
        
    }

    @And("Send a half message")
    public void sendAHalfMessage() {
        
    }

    @And("Create a transaction branch")
    public void createATransactionBranch() {
    }

    @And("Create a message, including the Topic\\({string}), Tag\\({string}), and Body\\({string})")
    public void createAMessageIncludingTheTopicTagAndBody(String arg0, String arg1, String arg2) {
        
    }

    @And("Create a message, including the Topic\\({string}), Tag\\({string}), Body\\({string}), deliveryTimestamp\\({string})")
    public void createAMessageIncludingTheTopicTagBodyDeliveryTimestamp(String arg0, String arg1, String arg2, String arg3) {
        
    }

    @And("Create a message, including the Topic\\({string}), Tag\\({string}), Body\\({string}), messageGroup\\({string})")
    public void createAMessageIncludingTheTopicTagBodyMessageGroup(String arg0, String arg1, String arg2, String arg3) {
        
    }

    @And("Create a message, including the Topic\\({string}), Tag\\({string}), Key\\({string}), Value\\({string}), Body\\({string}), msgKey\\({string})")
    public void createAMessageIncludingTheTopicTagKeyValueBodyMsgKey(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5) {

    }

    @And("Create a message, including the Topic\\({string}), and messageProperty\\({string}, {string})")
    public void createAMessageIncludingTheTopicAndMessageProperty(String arg0, String arg1, String arg2) {
    }

    @And("Create a message, including the Topic\\({string}), Body\\({string}), and messageProperty\\({string})")
    public void createAMessageIncludingTheTopicBodyAndMessageProperty(String arg0, String arg1, String arg2) {
        
    }

    @And("Create a message, including the Topic\\({string}), Body\\({string}), messageGroup\\({string}), and messageProperty\\({string})")
    public void createAMessageIncludingTheTopicBodyMessageGroupAndMessageProperty(String arg0, String arg1, String arg2, String arg3) {
    }

    @And("Set message {string} {string} times")
    public void setMessageTimes(String arg0, String arg1) {
        
    }

    @And("Set Key\\({string}), Value\\({string})")
    public void setKeyValue(String arg0, String arg1) {

    }

    @And("Set messageProperty {string} to {string} and {string} to {string}")
    public void setMessagePropertyToAndTo(String arg0, String arg1, String arg2, String arg3) {
    }
}
