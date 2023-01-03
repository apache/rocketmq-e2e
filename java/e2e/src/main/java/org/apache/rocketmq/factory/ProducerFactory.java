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

import org.apache.rocketmq.account.Account;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerFactory extends BaseFactory {
    private static Logger log = LoggerFactory.getLogger(ProducerFactory.class);

    public static RMQNormalProducer getRMQProducer(Account account, String topic) {
        try {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic)
                .build();
            return new RMQNormalProducer(producer);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static RMQNormalProducer getRMQTransProducer(Account account, String topic, TransactionChecker checker) {
        try {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic)
                .setTransactionChecker(checker)
                .build();
            return new RMQNormalProducer(producer);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Producer getProducer(Account account, String topic) {
        try {
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setTopics(topic)
                .build();
            return producer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
