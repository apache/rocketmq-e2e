package org.apache.rocketmq;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class ClientInitStepdefs {
    @And("Shutdown the producer and consumer")
    public void shutdownTheProducerAndConsumer() {
    }

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

    @And("Shutdown the producer")
    public void shutdownTheProducer() {

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

    @When("Create a PushConsumer, set the Endpoint\\({string}), ConsumerGroup\\({string}), SubscriptionExpression\\({string}), Topic\\({string}), MessageListener\\({string})")
    public void createAPushConsumerSetTheEndpointConsumerGroupSubscriptionExpressionTopicMessageListener(String arg0, String arg1, String arg2, String arg3, String arg4) {

    }

    @And("Create a Producer, set the Endpoint\\({string}), RequestTimeout:\\({string}), Topic\\({string}), TransactionChecker:\\({string})")
    public void createAProducerSetTheEndpointRequestTimeoutTopicTransactionChecker(String arg0, String arg1, String arg2, String arg3) {

    }

    @Then("Create a message, including the Topic\\({string}), SubscriptionExpression\\({string}), Key\\({string}), and Body\\({string})")
    public void createAMessageIncludingTheTopicSubscriptionExpressionKeyAndBody(String arg0, String arg1, String arg2, String arg3) {

    }

    @And("Create a Producer, set the Endpoint\\({string}), RequestTimeout:\\({string}), Topic\\({string})")
    public void createAProducerSetTheEndpointRequestTimeoutTopic(String arg0, String arg1, String arg2) {

    }

    @Then("Check send message failed")
    public void checkSendMessageFailed() {

    }
}
