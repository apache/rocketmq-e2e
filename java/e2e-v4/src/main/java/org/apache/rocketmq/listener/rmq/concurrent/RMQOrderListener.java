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

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.listener.AbstractListener;
import org.apache.rocketmq.utils.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RMQOrderListener extends AbstractListener implements MessageListenerOrderly {
    private static Logger logger = LoggerFactory.getLogger(RMQOrderListener.class);
    private ConsumeOrderlyStatus consumeStatus = ConsumeOrderlyStatus.SUCCESS;
    private AtomicInteger msgIndex = new AtomicInteger(0);
    private String listenerName;

    public RMQOrderListener() {
        this.listenerName = RandomUtils.getStringByUUID();
        logger.info("Start listening:{}", listenerName);
    }

    public RMQOrderListener(ConsumeOrderlyStatus consumeStatus) {
        this.consumeStatus = consumeStatus;
        this.listenerName = RandomUtils.getStringByUUID();
        logger.info("Start listening:{}", listenerName);
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        for (MessageExt message : msgs) {
            msgIndex.getAndIncrement();
            this.dequeueAllMessages.addData(message);
            this.dequeueMessages.addData(message);
            logger.info("{} - MessageId:{}, ReconsumeTimes:{}, Body:{}, tag:{}, recvIndex:{}, action:{}", listenerName, message.getMsgId(),
                    message.getReconsumeTimes(), new String(message.getBody()), message.getTags(), msgIndex.getAndIncrement() + 1, consumeStatus);
        }
        return consumeStatus;
    }
}
