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
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;

@Slf4j
@Component
@RocketMQTransactionListener
public class TransactionListenerImpl implements RocketMQLocalTransactionListener {

    private HashMap<String, String> receivedMessages = new HashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    private RocketMQLocalTransactionState executeState = RocketMQLocalTransactionState.COMMIT;
    private RocketMQLocalTransactionState checkState = RocketMQLocalTransactionState.COMMIT;

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object o) {
        log.info(message.toString());
        return executeState;
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        return checkState;
    }

    public void setExecuteState(RocketMQLocalTransactionState executeState) {
        log.info("executor state: {}", executeState);
        this.executeState = executeState;
    }

    public void setCheckState(RocketMQLocalTransactionState checkState) {
        log.info("check state: {}", checkState);
        this.checkState = checkState;
    }
}
