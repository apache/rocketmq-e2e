package org.apache.rocketmq;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;

public class DelayStepdefs {
    @And("Check consume all messages immediately")
    public void checkConsumeAllMessagesImmediately() {

    }

    @Then("Check consume all {int} messages after {int}s\\(±{int}s)")
    public void checkConsumeAllMessagesAfterSS(int arg0, int arg1, int arg2) {

    }

    @And("Set message {string} to {int} seconds before the current system time")
    public void setMessageToSecondsBeforeTheCurrentSystemTime(String arg0, int arg1) {

    }

    @And("Set message {string} to {int} seconds after the current system time")
    public void setMessageToSecondsAfterTheCurrentSystemTime(String arg0, int arg1) {

    }

    @And("Check the received message's {string} property {string} and value is expected")
    public void checkTheReceivedMessageSPropertyAndValueIsExpected(String arg0, String arg1) {

    }
}
