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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.AbstractMQConsumer;
import org.apache.rocketmq.listener.AbstractListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQIdempotentListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.listener.rmq.concurrent.RMQOrderListener;
import org.apache.rocketmq.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQNormalConsumer extends AbstractMQConsumer {
    private static Logger logger = LoggerFactory.getLogger(RMQNormalConsumer.class);

    private DefaultMQPushConsumer pushConsumer;
    private DefaultLitePullConsumer litePullConsumer;
    private DefaultMQPullConsumer pullConsumer;
    private AbstractListener listener = null;

    public RMQNormalConsumer(DefaultMQPushConsumer consumer) {
        this.pushConsumer = consumer;
    }

    public RMQNormalConsumer(DefaultLitePullConsumer consumer) {
        this.litePullConsumer = consumer;
    }

    public RMQNormalConsumer(DefaultMQPullConsumer consumer) {
        this.pullConsumer = consumer;
    }

    public void subscribeAndStart(String topic, String tag, RMQNormalListener listener) {
        Assertions.assertNotNull(pushConsumer);
        this.listener = listener;
        try {
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageListener(listener);
            pushConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, tag: {}", topic, tag);
    }

    public void subscribeAndStart(String topic, String tag, RMQIdempotentListener listener) {
        Assertions.assertNotNull(pushConsumer);
        this.listener = listener;
        try {
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageListener(listener);
            pushConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, tag: {}", topic, tag);
    }

    public void subscribeAndStartLitePull(String topic, String tag) {
        Assertions.assertNotNull(litePullConsumer);
        try {
            litePullConsumer.subscribe(topic, tag);
            litePullConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQLitePullConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQLitePullConsumer started - topic: {}, tag: {}", topic, tag);
    }

    public void startLitePullAssignMode() {
        Assertions.assertNotNull(litePullConsumer);
        try {
            litePullConsumer.setAutoCommit(false);
            litePullConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQLitePullConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQLitePullConsumer Assign Mode started");
    }

    public void startDefaultPull() {
        Assertions.assertNotNull(pullConsumer);
        try {
            pullConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPullConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPullConsumer started");
    }

    public void subscribeAndStartLitePull(String topic, MessageSelector messageSelector) {
        Assertions.assertNotNull(litePullConsumer);
        try {
            litePullConsumer.subscribe(topic, messageSelector);
            litePullConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQLitePullConsumer started - topic: {}, sql: {}", topic, messageSelector.getExpression());
    }

    public void subscribeAndStart(String topic, String tag, RMQOrderListener listener) {
        Assertions.assertNotNull(pushConsumer);
        this.listener = listener;
        try {
            pushConsumer.subscribe(topic, tag);
            pushConsumer.setMessageListener(listener);
            pushConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, tag: {}", topic, tag);
    }

    public void subscribeAndStart(String topic, MessageSelector messageSelector, RMQNormalListener listener) {
        Assertions.assertNotNull(pushConsumer);
        this.listener = listener;
        try {
            pushConsumer.subscribe(topic, messageSelector);
            pushConsumer.setMessageListener(listener);
            pushConsumer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQPushConsumer failed, {}", e.getMessage());
        }
        logger.info("DefaultMQPushConsumer started - topic: {}, messageSelector: {}", topic,
                messageSelector.getExpression());
        TestUtils.waitForSeconds(5);
    }

    @Override
    public void shutdown() {
        if (pushConsumer != null) {
            pushConsumer.shutdown();
            logger.info("DefaultMQPushConsumer shutdown !!!");
        }
        if (litePullConsumer != null) {
            litePullConsumer.shutdown();
            logger.info("DefaultLitePullConsumer shutdown !!!");
        }
    }

    public DefaultMQPushConsumer getPushConsumer() {
        return pushConsumer;
    }

    public void setPushConsumer(DefaultMQPushConsumer consumer) {
        this.pushConsumer = consumer;
    }

    public AbstractListener getListener() {
        return listener;
    }

    public void setListener(AbstractListener listener) {
        this.listener = listener;
    }

    public DefaultLitePullConsumer getLitePullConsumer() {
        return litePullConsumer;
    }

    public void setLitePullConsumer(DefaultLitePullConsumer consumer) {
        this.litePullConsumer = consumer;
    }

    public DefaultMQPullConsumer getPullConsumer() {
        return pullConsumer;
    }

    public void setPullConsumer(DefaultMQPullConsumer consumer) {
        this.pullConsumer = consumer;
    }
}
