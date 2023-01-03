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

package org.apache.rocketmq.broker.client.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.enums.TESTSET;
import org.apache.rocketmq.frame.BaseOperate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TESTSET.CLIENT)
public class ConsumerGroupTest extends BaseOperate {
    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupTest.class);
    private static String topic;
    private static String className;
    private SimpleConsumer consumer;

    @BeforeAll
    public static void setUpAll() {
        className = ConsumerGroupTest.class.getName();
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        topic = getTopic(TopicMessageType.NORMAL.getValue(), methodName);
    }

    @AfterEach
    public void tearDown() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @DisplayName("Use the built-in ConsumerGroup[DEFAULT_CONSUMER] to consume messages and expect consume failed")
    public void testSystemInnerConsumerGroup() {
        String groupId = "DEFAULT_CONSUMER";
        assertThrows(Exception.class, () -> {
            consumer = provider.newSimpleConsumerBuilder()
                .setConsumerGroup(groupId)
                .setAwaitDuration(Duration.ofSeconds(10))
                .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();
            consumer.receive(32, Duration.ofSeconds(10));
        }, "Expected Start [SimpleConsumer] Exception to throw, but it didn't");
    }

}
