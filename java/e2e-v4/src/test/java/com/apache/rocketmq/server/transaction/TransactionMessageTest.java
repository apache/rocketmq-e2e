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

package com.apache.rocketmq.server.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQTransactionProducer;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.factory.ConsumerFactory;
import org.apache.rocketmq.factory.ProducerFactory;
import org.apache.rocketmq.frame.BaseOperate;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.listener.rmq.concurrent.TransactionListenerImpl;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.NameUtils;
import org.apache.rocketmq.utils.VerifyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Tag(TESTSET.TRANSACTION)
@Tag(TESTSET.SMOKE)
public class TransactionMessageTest extends BaseOperate {
    private final Logger logger = LoggerFactory.getLogger(TransactionMessageTest.class);
    private String tag;
    private String topic;
    private String groupId;
    private final static int SEND_NUM = 10;

    @BeforeEach
    public void setUp() {
        topic = NameUtils.getTopicName();
        tag = NameUtils.getTagName();
        groupId = NameUtils.getGroupName();
        MQAdmin.createTopic(namesrvAddr, cluster, topic, 8);
        logger.info("topic:{}, tag:{}, groupId:{}", topic, tag, groupId);
    }

    @Test
    @DisplayName("同步发送10条普通消息，期望这10条消息被消费到")
    public void testConsumeNormalMessage() {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(namesrvAddr, groupId);
        consumer.subscribeAndStart(topic, tag, new RMQNormalListener());

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        RMQTransactionProducer producer = ProducerFactory.getTransProducer(namesrvAddr, executorService,
                new TransactionListenerImpl(LocalTransactionState.COMMIT_MESSAGE, LocalTransactionState.COMMIT_MESSAGE));
        producer.send(topic, tag, SEND_NUM);

        VerifyUtils.verifyNormalMessage(producer.getEnqueueMessages(), consumer.getListener().getDequeueMessages());
        producer.shutdown();
        consumer.shutdown();
    }

}
