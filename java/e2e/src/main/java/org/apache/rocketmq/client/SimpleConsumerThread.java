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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.common.MQCollector;

@Slf4j
public class SimpleConsumerThread extends MQCollector implements Runnable {

    private CountDownLatch latch;
    private SimpleConsumer simpleConsumer;
    private int maxMessageNum;
    private Duration invisibleDuration;
    private List<MessageView> viewList;
    private Boolean needRun = true;
    private AtomicInteger receivedIndex = new AtomicInteger(0);

    public SimpleConsumerThread(CountDownLatch latch, SimpleConsumer simpleConsumer, int maxMessageNum,
        Duration invisibleDuration) {
        this.latch = latch;
        this.simpleConsumer = simpleConsumer;
        this.maxMessageNum = maxMessageNum;
        this.invisibleDuration = invisibleDuration;
    }

    @Override
    public void run() {
        while (needRun) {
            final CompletableFuture<List<MessageView>> future = simpleConsumer.receiveAsync(maxMessageNum, invisibleDuration);
            future.thenAccept(new Consumer<List<MessageView>>() {
                @Override
                public void accept(List<MessageView> messageViews) {
                    log.info("received async success");
                    for (MessageView messageView : messageViews) {
                        dequeueMessages.addData(messageView);
                        log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                            StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                        CompletableFuture<Void> ackFuture = simpleConsumer.ackAsync(messageView);
                        ackFuture.thenAccept(new Consumer<Void>() {
                            @Override
                            public void accept(Void unused) {
                                log.info("ack async success");
                            }
                        });
                        ackFuture.exceptionally(new Function<Throwable, Void>() {
                            @Override
                            public Void apply(Throwable throwable) {
                                log.info("ack async failed");
                                return null;
                            }
                        });
                    }
                }
            });
            future.exceptionally(new Function<Throwable, List<MessageView>>() {
                @Override
                public List<MessageView> apply(Throwable throwable) {
                    log.info("received async failed");
                    return null;
                }
            });
            try {
                future.get();
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
