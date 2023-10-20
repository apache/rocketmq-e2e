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

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.listener.AbstractListener;
import org.apache.rocketmq.utils.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionListenerImpl extends AbstractListener implements TransactionListener {
    private static Logger logger = LoggerFactory.getLogger(TransactionListenerImpl.class);
    private LocalTransactionState checker = LocalTransactionState.COMMIT_MESSAGE;
    private LocalTransactionState executor = LocalTransactionState.COMMIT_MESSAGE;
    private int checkTimes = 1;
    private int checkNum = 0;
    private String checkerName;
    private AtomicInteger transactionIndex = new AtomicInteger(0);

    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    public TransactionListenerImpl() {
        this.checkerName = RandomUtils.getStringByUUID();
        logger.info("Start listening:{}", checkerName);
    }

    public TransactionListenerImpl(LocalTransactionState checker, LocalTransactionState executor) {
        this.checkerName = RandomUtils.getStringByUUID();
        this.checker = checker;
        this.executor = executor;
        logger.info("Start listening:{}", checkerName);
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
        logger.warn("TransactionId:{}, transactionStatus:{}", message.getTransactionId(), executor.name());

        // int value = transactionIndex.getAndIncrement();
        // int status = value % 3;
        // localTrans.put(message.getTransactionId(), status);
        return executor;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt message) {

        // if (checkTimes == 1) {
        // //消息 ID（有可能消息体一样，但消息 ID 不一样，当前消息属于半事务消息，所以消息 ID 在控制台无法查询）
        // String msgId = message.getMsgId();
        // logger.info("checker:{}, MessageId:{}, transactionStatus:{}", checkerName,
        // msgId, checker.name());
        // return checker;
        // }
        // if (new String(message.getBody()).equals(new String(sendMsg.getBody()))) {
        // checkNum++;
        // //check次数达到预期，则提交
        // if (checkNum == checkTimes) {
        // checker = LocalTransactionState.COMMIT_MESSAGE;
        // }
        // logger.info("checkerName:{}, MessageId:{}, status:{}, checkedTimes:{},
        // expectCheckNum:{}", checkerName, message.getMsgId(), checker, checkTimes,
        // checkNum);
        // }
        // Integer status = localTrans.get(message.getTransactionId());
        // if (null != status) {
        // switch (status) {
        // case 0:
        // return LocalTransactionState.UNKNOW;
        // case 1:
        // return LocalTransactionState.COMMIT_MESSAGE;
        // case 2:
        // return LocalTransactionState.ROLLBACK_MESSAGE;
        // }
        // }
        // 本地事务已成功则提交消息
        return checker;
    }
}
