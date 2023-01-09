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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.AbstractMQConsumer;
import org.apache.rocketmq.listener.AbstractListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQOrderListener;
import org.apache.rocketmq.utils.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQNormalConsumer extends AbstractMQConsumer {
    private static Logger logger = LoggerFactory.getLogger(RMQNormalConsumer.class);

    private DefaultMQPushConsumer consumer;
    private AbstractListener listener = null;

    public RMQNormalConsumer(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }

    public void subscribeAndStart(String topic, String tag, RMQNormalListener listener) {
        this.listener = listener;
        try {
            consumer.subscribe(topic, tag);
            consumer.setMessageListener(listener);
            consumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, tag: {}", topic, tag);
    }

    public void subscribeAndStart(String topic, String tag, RMQOrderListener listener) {
        this.listener = listener;
        try {
            consumer.subscribe(topic, tag);
            consumer.setMessageListener(listener);
            consumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, tag: {}", topic, tag);
    }

    public void subscribeAndStart(String topic, MessageSelector messageSelector, RMQNormalListener listener) {
        this.listener = listener;
        try {
            consumer.subscribe(topic, messageSelector);
            consumer.setMessageListener(listener);
            consumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, messageSelector: {}", topic, messageSelector.getExpression());
        TestUtils.waitForSeconds(5);
    }

    @Override
    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
            logger.info("DefaultMQPushConsumer shutdown !!!");
        }
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }

    public AbstractListener getListener() {
        return listener;
    }

    public void setListener(AbstractListener listener) {
        this.listener = listener;
    }
}
