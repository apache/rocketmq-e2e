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

package org.apache.rocketmq.client.rmq;

import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.common.AbstractMQProducer;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.util.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class RMQNormalProducer extends AbstractMQProducer {
    private static Logger log = LoggerFactory.getLogger(RMQNormalProducer.class);
    private Producer producer;

    public RMQNormalProducer(Producer producer) {
        this.producer = producer;
    }

    public Producer getProducer() {
        return producer;
    }

    public void send(org.apache.rocketmq.client.apis.message.Message message) {
        try {
            SendReceipt sendReceipt = this.producer.send(message);
            log.info("{}, MessageGroup: {}, Topic: {}", sendReceipt.getMessageId(), message.getMessageGroup().isPresent() ? message.getMessageGroup().get() : "", message.getTopic());
            this.enqueueMessages.addData(sendReceipt.getMessageId().toString());
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Producer send message failed, " + e.getMessage());
        }
    }

    public void sendTrans(org.apache.rocketmq.client.apis.message.Message message, Boolean commit) {
        log.info("Producer start to send messages");
        try {
            Transaction transaction = producer.beginTransaction();
            SendReceipt sendReceipt = producer.send(message, transaction);
            if (commit != null) {
                if (commit) {
                    transaction.commit();
                    log.info("exec commit()");
                } else {
                    transaction.rollback();
                    log.info("exec rollback()");
                }
            }
            log.info("{}", sendReceipt);
            this.enqueueMessages.addData(sendReceipt.getMessageId().toString());
        } catch (Exception e) {
            log.error("Producer send message failed, {}", e.getMessage());
        }
        log.info("Producer send messages finished");
    }

    public void send(String topic, String tag, int messageNum) {
        log.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            org.apache.rocketmq.client.apis.message.Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            try {
                SendReceipt sendReceipt = producer.send(message);
                this.enqueueMessages.addData(sendReceipt.getMessageId().toString());
            } catch (Exception e) {
                log.error("Producer send message failed, {}, topic:{}, tag:{}", e.getMessage(), topic, tag);
            }
        }
        log.info("Producer send messages finished");
    }

    public void send(String topic, String tag, long delaySecondTime, int messageNum) {
        log.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            org.apache.rocketmq.client.apis.message.Message message = MessageFactory.buildDelayMessage(topic, tag, RandomUtils.getStringByUUID(), System.currentTimeMillis() + delaySecondTime * 1000);
            try {
                SendReceipt sendReceipt = producer.send(message);
                log.info("{}, index: {}", sendReceipt, i);
                this.enqueueMessages.addData(sendReceipt.getMessageId().toString());
            } catch (Exception e) {
                log.error("Producer send message failed, {}", e.getMessage());
            }
        }
        log.info("Producer send messages finished");
    }

    public void sendAsync(String topic, String tag, int messageNum) {
        log.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            org.apache.rocketmq.client.apis.message.Message message = MessageFactory.buildMessage(topic, tag, RandomUtils.getStringByUUID());
            try {
                CompletableFuture<SendReceipt> future = producer.sendAsync(message);
                future.thenAccept(new Consumer<SendReceipt>() {
                    @Override
                    public void accept(SendReceipt receipt) {
                        log.info("callback msg success: {}", receipt.getMessageId());
                        enqueueMessages.addData(receipt.getMessageId().toString());
                    }
                });
                future.exceptionally(new Function<Throwable, SendReceipt>() {
                    @Override
                    public SendReceipt apply(Throwable throwable) {
                        log.warn("{} callback message failed", message.getTopic());
                        enqueueFailedMessages.addData(message);
                        return null;
                    }
                });
                log.info("{}, index: {}", future, i);
                future.get();
            } catch (Exception e) {
                log.error("Producer async send message failed, {}", e.getMessage());
            }
        }
        log.info("Producer async send messages finished");
    }

    public void sendAsync(Message message) {
        try {
            CompletableFuture<SendReceipt> future = producer.sendAsync(message);
            future.thenAccept(new Consumer<SendReceipt>() {
                @Override
                public void accept(SendReceipt receipt) {
                    log.info("Async send success: {}, topic:{}, tag:{}, body:{}", receipt.getMessageId(), message.getTopic(), message.getTag(), StandardCharsets.UTF_8.decode(message.getBody()));
                    enqueueMessages.addData(receipt.getMessageId().toString());
                }
            });
            future.exceptionally(new Function<Throwable, SendReceipt>() {
                @Override
                public SendReceipt apply(Throwable throwable) {
                    log.warn("send {} callback message failed", message.getTopic());
                    enqueueFailedMessages.addData(message);
                    return null;
                }
            });
            future.get();
        } catch (Exception e) {
            log.error("Producer async send message failed, {}", e.getMessage());
        }
    }

    public void close() {
        if (producer != null) {
            try {
                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("Producer shutdown !!!");
        }
    }

}
