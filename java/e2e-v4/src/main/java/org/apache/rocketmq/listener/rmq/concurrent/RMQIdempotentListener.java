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

package org.apache.rocketmq.listener.rmq.concurrent;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.listener.AbstractListener;
import org.apache.rocketmq.utils.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RMQIdempotentListener extends AbstractListener implements MessageListenerConcurrently {
    private static Logger logger = LoggerFactory.getLogger(RMQNormalListener.class);
    private ConsumeConcurrentlyStatus consumeStatus = ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    private AtomicInteger msgIndex = new AtomicInteger(0);
    private String listenerName;
    private MessageIdStore messageIdStore = MessageIdStore.getInstance();

    public RMQIdempotentListener() {
        this.listenerName = RandomUtils.getStringByUUID();
        logger.info("Start listening:{}", listenerName);
    }

    public RMQIdempotentListener(String listenerName) {
        this.listenerName = listenerName;
        logger.info("Start listening:{}", listenerName);
    }

    public RMQIdempotentListener(ConsumeConcurrentlyStatus consumeStatus) {
        this.consumeStatus = consumeStatus;
        this.listenerName = RandomUtils.getStringByUUID();
        logger.info("Start listening:{}", listenerName);
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        for (MessageExt message : msgs) {
            boolean isConsumed = messageIdStore.isMessageConsumed(message.getMsgId());
            if (isConsumed) {
                return consumeStatus;
            } else {
                msgIndex.getAndIncrement();
                message.putUserProperty("startDeliverTime", String.valueOf(System.currentTimeMillis()));
                this.dequeueAllMessages.addData(message);
                this.dequeueMessages.addData(message);
                logger.info("{} - MessageId:{}, ReconsumeTimes:{}, Body:{}, tag:{}, recvIndex:{}, action:{}",
                        listenerName, message.getMsgId(),
                        message.getReconsumeTimes(), new String(message.getBody()), message.getTags(),
                        msgIndex.getAndIncrement() + 1, consumeStatus);
                messageIdStore.markMessageAsConsumed(message.getMsgId());
            }
        }
        return consumeStatus;
    }
}
