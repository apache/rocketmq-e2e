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

package org.apache.rocketmq.common;

import org.apache.rocketmq.util.RandomUtils;
import org.apache.rocketmq.util.data.collect.DataCollector;
import org.apache.rocketmq.util.data.collect.DataCollectorManager;

public abstract class MQCollector {
    /**
     * messages of enqueue successfully(sent successfully)
     */
    protected DataCollector<Object> enqueueMessages = null;
    protected DataCollector<Object> enqueueFailedMessages = null;
    /**
     * messages of dequeue successfully(Consumption return success)
     */
    protected volatile DataCollector<Object> dequeueMessages = null;
    /**
     * all messages of dequeue successfully(Contains retry message)
     */
    protected DataCollector<Object> dequeueAllMessages = null;
    /**
     * the message body of dequeue successfully(No repetition)
     */
    protected DataCollector<String> dequeueUndupMessageBody = null;
    //Message sending response time collector
    protected DataCollector msgRTs = null;

    public MQCollector() {
        enqueueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        enqueueFailedMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        dequeueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        dequeueAllMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        dequeueUndupMessageBody = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());

        msgRTs = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
    }

    public void clearMsg() {
        enqueueMessages.resetData();
        enqueueFailedMessages.resetData();
        dequeueMessages.resetData();
        dequeueAllMessages.resetData();
        dequeueUndupMessageBody.resetData();
        msgRTs.resetData();
    }

    public void lockCollectors() {
        enqueueMessages.lockIncrement();
        enqueueFailedMessages.lockIncrement();
        dequeueMessages.lockIncrement();
        dequeueAllMessages.lockIncrement();
        dequeueUndupMessageBody.lockIncrement();
        msgRTs.lockIncrement();
    }

    public DataCollector<Object> getEnqueueMessages() {
        return enqueueMessages;
    }

    public void setEnqueueMessages(DataCollector<Object> enqueueMessages) {
        this.enqueueMessages = enqueueMessages;
    }

    public DataCollector<Object> getEnqueueFailedMessages() {
        return enqueueFailedMessages;
    }

    public void setEnqueueFailedMessages(
        DataCollector<Object> enqueueFailedMessages) {
        this.enqueueFailedMessages = enqueueFailedMessages;
    }

    public DataCollector<Object> getDequeueMessages() {
        return dequeueMessages;
    }

    public void setDequeueMessages(DataCollector<Object> dequeueMessages) {
        this.dequeueMessages = dequeueMessages;
    }

    public DataCollector<Object> getDequeueAllMessages() {
        return dequeueAllMessages;
    }

    public void setDequeueAllMessages(DataCollector<Object> dequeueAllMessages) {
        this.dequeueAllMessages = dequeueAllMessages;
    }

    public DataCollector<String> getDequeueUndupMessageBody() {
        return dequeueUndupMessageBody;
    }

    public void setDequeueUndupMessageBody(
        DataCollector<String> dequeueUndupMessageBody) {
        this.dequeueUndupMessageBody = dequeueUndupMessageBody;
    }

    public DataCollector getMsgRTs() {
        return msgRTs;
    }

    public void setMsgRTs(DataCollector msgRTs) {
        this.msgRTs = msgRTs;
    }
}
