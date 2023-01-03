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
package org.apache.rocketmq.listener.rmq;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.common.MQCollector;
import org.apache.rocketmq.util.RandomUtils;
import org.apache.rocketmq.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQNormalListener extends MQCollector implements MessageListener {
    private static Logger logger = LoggerFactory.getLogger(RMQNormalListener.class);
    private ConsumeResult consumeStatus = ConsumeResult.SUCCESS;
    private AtomicInteger msgIndex = new AtomicInteger(0);
    private String listenerName;
    private int reconsumeTimes = 0;
    private boolean isEvenNumber = false;
    private int workTime = 0;

    public RMQNormalListener() {
        this.listenerName = RandomUtils.getStringByUUID();
        logger.info("start listener:{}", listenerName);
    }

    public RMQNormalListener(int reconsumeTimes, ConsumeResult consumeStatus) {
        this.listenerName = RandomUtils.getStringByUUID();
        this.consumeStatus = consumeStatus;
        this.reconsumeTimes = reconsumeTimes;
        logger.info("start listener:{}", listenerName);
    }

    public RMQNormalListener(int reconsumeTimes, int workTime) {
        this.listenerName = RandomUtils.getStringByUUID();
        this.workTime = workTime;
        this.reconsumeTimes = reconsumeTimes;
        logger.info("start listener:{}", listenerName);
    }

    public RMQNormalListener(String listenerName) {
        this.listenerName = listenerName;
        logger.info("start listener:{}", listenerName);
    }

    public RMQNormalListener(ConsumeResult consumeStatus) {
        this.consumeStatus = consumeStatus;
        this.listenerName = RandomUtils.getStringByUUID();
        logger.info("start listener:{}", listenerName);
    }

    @Override
    public ConsumeResult consume(MessageView view) {
        ConsumeResult result = consumeStatus;
        if (reconsumeTimes == 0 || reconsumeTimes == view.getDeliveryAttempt() - 1) {
            this.dequeueMessages.addData(view);
            result = consumeStatus;
        } else {
            if (isEvenNumber) {
                if (Integer.parseInt(view.getBody().toString()) % 2 != 0) {
                    this.dequeueMessages.addData(view);
                    result = ConsumeResult.SUCCESS;
                }
            }
            TestUtils.waitForSeconds(workTime);
        }

        logger.info("{} - MessageId:{}, body:{}, tag:{},  key:{}, recvIndex:{}, property:{}, action:{}", listenerName, view.getMessageId(), StandardCharsets.UTF_8.decode(view.getBody()), view.getTag().isPresent() ? view.getTag().get() : "", view.getKeys(), msgIndex.getAndIncrement() + 1, view.getProperties(), consumeStatus);
        return result;
    }
}
