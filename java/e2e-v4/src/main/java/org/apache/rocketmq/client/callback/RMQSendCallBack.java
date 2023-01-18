/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.client.callback;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.utils.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQSendCallBack implements SendCallback {
    private static Logger logger = LoggerFactory.getLogger(RMQSendCallBack.class);
    private final static int WAIT_RESPONSE_MILLS = 15 * 1000;
    private boolean isDebug = false;
    private boolean bSuccessResponse = false;
    private boolean bFailResponse = false;
    private String messageId = null;

    public RMQSendCallBack(boolean isDebug) {
        this.isDebug = isDebug;
    }

    public RMQSendCallBack() {
    }

    @Override
    public void onSuccess(SendResult sendResult) {
        logger.info("{} callback msg success: {}", sendResult.getMessageQueue(), sendResult.getMsgId());
        messageId = sendResult.getMsgId();
        bSuccessResponse = true;
    }

    @Override
    public void onException(Throwable e) {
        //logger.warn("{} callback message failed: {}  exception: {}", context.getTopic(), context.getMessageId(), context.getException());
        bFailResponse = true;
    }

    public void waitResponse() {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < WAIT_RESPONSE_MILLS && (!bSuccessResponse && !bFailResponse)) {
            TestUtils.waitForMoment(100);
        }
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean debug) {
        isDebug = debug;
    }

    public boolean isbSuccessResponse() {
        return bSuccessResponse;
    }

    public void setbSuccessResponse(boolean bSuccessResponse) {
        this.bSuccessResponse = bSuccessResponse;
    }

    public boolean isbFailResponse() {
        return bFailResponse;
    }

    public void setbFailResponse(boolean bFailResponse) {
        this.bFailResponse = bFailResponse;
    }
}
