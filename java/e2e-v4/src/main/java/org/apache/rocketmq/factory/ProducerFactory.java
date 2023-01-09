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

package org.apache.rocketmq.factory;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.client.rmq.RMQTransactionProducer;
import org.apache.rocketmq.utils.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class ProducerFactory {
    private static Logger logger = LoggerFactory.getLogger(ProducerFactory.class);

    public static RMQNormalProducer getRMQProducer(String ns) {
        DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID());
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(ns);
        try {
            producer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        return new RMQNormalProducer(producer);
    }

    public static RMQNormalProducer getOrderProducer(String ns) {
        DefaultMQProducer producer = new DefaultMQProducer(RandomUtils.getStringByUUID());
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(ns);
        try {
            producer.start();
        } catch (MQClientException e) {
            logger.info("Start DefaultMQProducer failed, {}", e.getMessage());
        }
        return new RMQNormalProducer(producer);
    }

    public static RMQTransactionProducer getTransProducer(String ns, ExecutorService executorService, TransactionListener transactionListener) {
        TransactionMQProducer producer = new TransactionMQProducer(RandomUtils.getStringByUUID());
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(ns);
        try {
            if (executorService != null) {
                producer.setExecutorService(executorService);
            }
            producer.setTransactionListener(transactionListener);
            producer.start();
        } catch (MQClientException e) {
            logger.info("Start TransactionMQProducer failed, {}", e.getMessage());
        }
        return new RMQTransactionProducer(producer);
    }

}
