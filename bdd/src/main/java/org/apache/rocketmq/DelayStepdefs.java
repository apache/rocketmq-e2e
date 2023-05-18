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
