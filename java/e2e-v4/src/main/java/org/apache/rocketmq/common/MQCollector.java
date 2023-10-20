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

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.utils.RandomUtils;
import org.apache.rocketmq.utils.data.collect.DataCollector;
import org.apache.rocketmq.utils.data.collect.DataCollectorManager;

public abstract class MQCollector {
    /**
     * 成功入队的消息（发送成功）
     */
    protected DataCollector<MessageExt> enqueueMessages = null;
    protected DataCollector<MessageExt> enqueueFailedMessages = null;
    /**
     * 成功出队的消息（消费返回成功）
     */
    protected DataCollector<MessageExt> dequeueMessages = null;
    /**
     * 全部出队的消息（包含重试消息）
     */
    protected DataCollector<MessageExt> dequeueAllMessages = null;
    /**
     * 全部出队的消息体（去重）
     */
    protected DataCollector<String> dequeueUndupMessageBody = null;
    // 消息发送响应时间收集器
    protected DataCollector msgRTs = null;

    public MQCollector() {
        enqueueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        enqueueFailedMessages = DataCollectorManager.getInstance()
                .fetchListDataCollector(RandomUtils.getStringByUUID());
        dequeueMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        dequeueAllMessages = DataCollectorManager.getInstance().fetchListDataCollector(RandomUtils.getStringByUUID());
        dequeueUndupMessageBody = DataCollectorManager.getInstance()
                .fetchListDataCollector(RandomUtils.getStringByUUID());

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

    public DataCollector<MessageExt> getEnqueueMessages() {
        return enqueueMessages;
    }

    public void setEnqueueMessages(DataCollector<MessageExt> enqueueMessages) {
        this.enqueueMessages = enqueueMessages;
    }

    public DataCollector<MessageExt> getEnqueueFailedMessages() {
        return enqueueFailedMessages;
    }

    public void setEnqueueFailedMessages(DataCollector<MessageExt> enqueueFailedMessages) {
        this.enqueueFailedMessages = enqueueFailedMessages;
    }

    public DataCollector<MessageExt> getDequeueMessages() {
        return dequeueMessages;
    }

    public void setDequeueMessages(DataCollector<MessageExt> dequeueMessages) {
        this.dequeueMessages = dequeueMessages;
    }

    public DataCollector<MessageExt> getDequeueAllMessages() {
        return dequeueAllMessages;
    }

    public void setDequeueAllMessages(DataCollector<MessageExt> dequeueAllMessages) {
        this.dequeueAllMessages = dequeueAllMessages;
    }

    public DataCollector<String> getDequeueUndupMessageBody() {
        return dequeueUndupMessageBody;
    }

    public void setDequeueUndupMessageBody(DataCollector<String> dequeueUndupMessageBody) {
        this.dequeueUndupMessageBody = dequeueUndupMessageBody;
    }

    public DataCollector getMsgRTs() {
        return msgRTs;
    }

    public void setMsgRTs(DataCollector msgRTs) {
        this.msgRTs = msgRTs;
    }
}
