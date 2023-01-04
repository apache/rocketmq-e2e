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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.rocketmq.client.SimpleConsumerThread;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.MQCollector;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQNormalConsumer extends MQCollector {
    private static final Logger log = LoggerFactory.getLogger(RMQNormalConsumer.class);
    private PushConsumer pushConsumer;
    private SimpleConsumer simpleConsumer;
    private DefaultMQPushConsumer consumer;
    private ExecutorService executorService = Executors.newFixedThreadPool(20);
    private final static int WAIT_RESPONSE_MILLS = 15 * 1000;
    private RMQNormalListener listener = null;
    private Duration invisibleDuration = null;
    private int waitTimeoutSec = 60;
    private Boolean needRun = true;
    private boolean bSuccessResponse = false;
    private boolean bFailResponse = false;
    private boolean ackSuccessResponse = false;
    private boolean ackFailResponse = false;
    private int consumerThreadNum = 20;
    private static volatile AtomicInteger receivedIndex = new AtomicInteger(0);

    public RMQNormalConsumer(PushConsumer consumer, RMQNormalListener listener) {
        this.listener = listener;
        this.pushConsumer = consumer;
    }

    public RMQNormalConsumer(SimpleConsumer consumer) {
        this.simpleConsumer = consumer;
    }

    public void receive(int maxMessageNum, int expectedNum, Duration invisibleDuration, long timeout) {
        int recvNum = 0;
        long start = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - start < timeout) {
                final List<MessageView> messageViews = simpleConsumer.receive(maxMessageNum, invisibleDuration);
                log.info("simple receive message: {}", messageViews.size());
                for (MessageView messageView : messageViews) {
                    receivedIndex.getAndIncrement();
                    recvNum++;
                    dequeueMessages.addData(messageView);
                    log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}", messageView.getMessageId(),
                        StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get());
                    simpleConsumer.ack(messageView);
                }
                if (recvNum == expectedNum) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }
    }

    public void receiveThenAck(int maxMessageNum, int expectedNum, Duration invisibleDuration, long timeout) {
        new Thread(() -> {
            int recvNum = 0;
            long start = System.currentTimeMillis();
            try {
                while (System.currentTimeMillis() - start < timeout) {
                    final List<MessageView> messageViews = simpleConsumer.receive(maxMessageNum, invisibleDuration);
                    log.info("simple receive message: {}", messageViews.size());
                    for (MessageView messageView : messageViews) {
                        receivedIndex.getAndIncrement();
                        recvNum++;
                        dequeueMessages.addData(messageView);
                        log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}", messageView.getMessageId(),
                            StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get());
                        simpleConsumer.ack(messageView);
                    }
                    if (recvNum == expectedNum) {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail(e.getMessage());
            }
        }).start();
    }

    public void receiveThenAck(int maxMessageNum, Duration invisibleDuration) {
        for (int i = 0; i < consumerThreadNum; i++) {
            executorService.submit(() -> {
                try {
                    log.info("needRun:{}", needRun);
                    while (needRun) {
                        final List<MessageView> messageViews = simpleConsumer.receive(maxMessageNum, invisibleDuration);
                        log.info("simple receive message: {}", messageViews.size());
                        for (MessageView messageView : messageViews) {
                            receivedIndex.getAndIncrement();
                            dequeueMessages.addData(messageView);
                            log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                            simpleConsumer.ack(messageView);
                            log.info("ack success");
                        }
                    }
                } catch (ClientException e) {
                    e.printStackTrace();
                    Assertions.fail(e.getMessage());
                }
            });
        }
    }

    public void receiveAsyncThenAck(int maxMessageNum, Duration invisibleDuration) {
        for (int i = 0; i < consumerThreadNum; i++) {
            final CompletableFuture<List<MessageView>> future = simpleConsumer.receiveAsync(maxMessageNum, invisibleDuration);

            future.thenAccept(new Consumer<List<MessageView>>() {
                @Override
                public void accept(List<MessageView> messageViews) {
                    log.info("receive {} messages :{}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                    for (MessageView messageView : messageViews) {
                        dequeueMessages.addData(messageView);
                        receivedIndex.getAndIncrement();
                        log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                            StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                        try {
                            simpleConsumer.ack(messageView);
                            log.info("ack message: {} success", messageView.getMessageId());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            future.exceptionally(new Function<Throwable, List<MessageView>>() {
                @Override
                public List<MessageView> apply(Throwable throwable) {
                    throwable.printStackTrace();
                    log.info("received async failed: {}", throwable.getMessage());
                    return null;
                }
            });
        }
    }

    public void receiveThenAckAsync(int maxMessageNum, Duration invisibleDuration) {
        for (int i = 0; i < consumerThreadNum; i++) {
            executorService.submit(() -> {
                try {
                    while (needRun) {
                        final List<MessageView> messageViews = simpleConsumer.receive(maxMessageNum, invisibleDuration);
                        for (MessageView messageView : messageViews) {
                            this.dequeueMessages.addData(messageView);
                            receivedIndex.getAndIncrement();
                            log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                            CompletableFuture<Void> future = simpleConsumer.ackAsync(messageView);
                            future.thenAccept(new Consumer<Void>() {
                                @Override
                                public void accept(Void unused) {
                                    ackSuccessResponse = true;
                                    log.info("ack async success");
                                }
                            });
                            future.exceptionally(new Function<Throwable, Void>() {
                                @Override
                                public Void apply(Throwable throwable) {
                                    ackFailResponse = true;
                                    Assertions.fail("ackAsync failed! " + throwable.getMessage());
                                    return null;
                                }
                            });
                            try {
                                future.get();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (ClientException e) {
                    Assertions.fail(e.getMessage());
                }
            });
        }
    }

    public void receiveAsyncThenAckAsync(int maxMessageNum, Duration invisibleDuration) {
        CountDownLatch latch = new CountDownLatch(consumerThreadNum);
        for (int i = 0; i < consumerThreadNum; i++) {
            log.info("=======================");
            SimpleConsumerThread consumerThread = new SimpleConsumerThread(latch, simpleConsumer, 10, Duration.ofSeconds(60));
            executorService.submit(consumerThread);
            this.dequeueMessages.addData(consumerThread.getDequeueMessages());
        }
        try {
            latch.await();
            log.info("The execution of all child threads is complete");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * No ack operation after a message is received
     *
     * @param maxMessageNum
     * @param invisibleDuration
     */
    public void receiveThenNack(int maxMessageNum, Duration invisibleDuration) {
        receiveThenNack(maxMessageNum, invisibleDuration, null);
    }

    public void receiveThenNack(int maxMessageNum, Duration receiveInvisibleDuration,
        Duration changeInvisibleDuration) {
        for (int i = 0; i < consumerThreadNum; i++) {
            executorService.submit(() -> {
                try {
                    while (needRun) {
                        final List<MessageView> messageViews = simpleConsumer.receive(maxMessageNum, receiveInvisibleDuration);
                        for (MessageView messageView : messageViews) {
                            this.dequeueMessages.addData(messageView);
                            log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                            if (changeInvisibleDuration != null) {
                                simpleConsumer.changeInvisibleDuration(messageView, changeInvisibleDuration);
                            }
                        }
                    }
                } catch (ClientException e) {
                    e.printStackTrace();
                    Assertions.fail(e.getMessage());
                }
            });
        }
    }

    public void close() {
        if (pushConsumer != null) {
            try {
//                log.info("DefaultMQPushConsumer shutdown start!!!");
                pushConsumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("DefaultMQPushConsumer shutdown !!!");
        }
        if (simpleConsumer != null) {
            try {
                simpleConsumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("SimpleConsumer shutdown !!!");
        }
    }

    public PushConsumer getPushConsumer() {
        return pushConsumer;
    }

    public void setPushConsumer(PushConsumer pushConsumer) {
        this.pushConsumer = pushConsumer;
    }

    public SimpleConsumer getSimpleConsumer() {
        return simpleConsumer;
    }

    public void setSimpleConsumer(SimpleConsumer simpleConsumer) {
        this.simpleConsumer = simpleConsumer;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }

    public RMQNormalListener getListener() {
        return listener;
    }

    public void setListener(RMQNormalListener listener) {
        this.listener = listener;
    }
}
