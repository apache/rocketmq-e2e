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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.test.topic.order}", consumerGroup = "gid-order-spring-test", selectorExpression = "*", consumeMode = ConsumeMode.ORDERLY)
public class OrderListenerImpl implements RocketMQListener<MessageExt> {

    private ConcurrentHashMap<String, LinkedList<MessageExt>> messageGroups = new ConcurrentHashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    @Override
    public void onMessage(MessageExt msg) {
        log.info("Received message- queueId:{}, msgId:{}, body:{}", msg.getQueueId(), msg.getMsgId(), new String(msg.getBody()));
        lastMessage = msg.getMsgId();
        String shardingKey = String.valueOf(msg.getQueueId());
        LinkedList<MessageExt> messages = new LinkedList<>();
        if (messageGroups.containsKey(shardingKey)) {
            messages = messageGroups.get(shardingKey);
            messages.add(msg);
        } else {
            messages.add(msg);
            messageGroups.put(shardingKey, messages);
        }
        receivedMessageIds.add(msg.getMsgId());
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public ConcurrentHashMap<String, LinkedList<MessageExt>> getMessageGroups() {
        return messageGroups;
    }

    public LinkedList<String> getReceivedMessageIds() {
        return receivedMessageIds;
    }
}
