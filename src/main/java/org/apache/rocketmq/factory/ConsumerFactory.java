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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.rocketmq.account.Account;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.listener.rmq.RMQNormalListener;
import org.apache.rocketmq.util.TestUtils;
import org.junit.jupiter.api.Assertions;

public class ConsumerFactory extends BaseFactory {

    public static RMQNormalConsumer getRMQPushConsumer(Account account, String topic, String consumerGroup,
        FilterExpression filterExpression, RMQNormalListener messageListener) {
        PushConsumer pushConsumer = null;
        try {
            pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setMessageListener(messageListener)
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new RMQNormalConsumer(pushConsumer, messageListener);
    }

    public static PushConsumer getPushConsumer(Account account, String topic, String consumerGroup,
        FilterExpression filterExpression, RMQNormalListener messageListener) {
        PushConsumer pushConsumer = null;
        try {
            pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setMessageListener(messageListener)
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pushConsumer;
    }

    public static PushConsumer getPushConsumer(Account account, String consumerGroup,
        Map<String, FilterExpression> subscriptionExpressions,
        RMQNormalListener messageListener) {
        PushConsumer pushConsumer = null;
        try {
            pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(subscriptionExpressions)
                .setMessageListener(messageListener)
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pushConsumer;
    }

    public static RMQNormalConsumer getRMQSimpleConsumer(Account account, String topic, String consumerGroup,
        FilterExpression filterExpression, Duration awaitDuration) {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(consumerGroup)
                .setAwaitDuration(awaitDuration)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new RMQNormalConsumer(simpleConsumer);
    }

    public static SimpleConsumer getSimpleConsumer(Account account, String topic, String consumerGroup,
        FilterExpression filterExpression, Duration awaitDuration) {
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(ClientConfigurationFactory.build(account))
                .setConsumerGroup(consumerGroup)
                .setAwaitDuration(awaitDuration)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .build();
            TestUtils.waitForSeconds(1);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("start [SimpleConsumer] failed, message: " + e.getMessage());
        }

        return simpleConsumer;
    }
}
