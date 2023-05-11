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
package org.apache.rocketmq.message;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.SpringBootBaseTest;
import org.apache.rocketmq.client.OrderListenerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Slf4j
public class OrderMessageTest extends SpringBootBaseTest {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    @Autowired
    private OrderListenerImpl listener;
    @Value("${rocketmq.test.topic.order}")
    private String orderTopic;

    @Test
    public void testSendOrderly() {
        int msgSize = 50;
        for (int i = 0; i < msgSize; i++) {
            String hash = String.valueOf(i % 5);
            SendResult sendResult = rocketMQTemplate.syncSendOrderly(orderTopic, i, hash);
            log.info("Send msgId:{}, queue:{}, hash:{}", sendResult.getMsgId(), sendResult.getMessageQueue(), hash);
            Assertions.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        }
        await().atMost(30, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return checkOrderMessage(listener.getMessageGroups());
            }
        });
    }

    public static boolean checkOrderMessage(ConcurrentHashMap<String, LinkedList<MessageExt>> receivedMessage) {
        for (Map.Entry<String, LinkedList<MessageExt>> stringLinkedListEntry : receivedMessage.entrySet()) {
            StringBuilder sb = new StringBuilder(String.format("shardingKey %s,message order: ", stringLinkedListEntry.getKey()));
            int preNode = -1;
            LinkedList<MessageExt> messages = stringLinkedListEntry.getValue();
            for (MessageExt message : messages) {
                int curNode = Integer.parseInt(new String(message.getBody()));
                sb.append(curNode).append(",");
                if (preNode > curNode) {
                    log.error(sb.toString());
                    return false;
                }
                preNode = curNode;
            }
        }
        return true;
    }
}
