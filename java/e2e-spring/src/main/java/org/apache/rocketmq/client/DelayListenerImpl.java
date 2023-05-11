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
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;
@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.test.topic.delay}", consumerGroup = "gid-delay-spring-test", selectorExpression = "*")
public class DelayListenerImpl implements RocketMQListener<MessageExt> {

    //  <MessageId, ReceivedTime>
    private HashMap<String, Long> receivedMessages = new HashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    @Override
    public void onMessage(MessageExt msg) {
        log.info("Received message: " + msg);
        lastMessage = msg.getMsgId();
        receivedMessages.put(msg.getMsgId(), System.currentTimeMillis());
        receivedMessageIds.add(msg.getMsgId());
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public HashMap<String, Long> getReceivedMessages() {
        return receivedMessages;
    }

    public LinkedList<String> getReceivedMessageIds() {
        return receivedMessageIds;
    }
}
