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

package org.apache.rocketmq.frame;

import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.util.MQAdmin;
import org.apache.rocketmq.util.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseOperate extends ResourceInit {

    private static final Logger log = LoggerFactory.getLogger(BaseOperate.class);
    protected static ClientServiceProvider provider = ClientServiceProvider.loadService();

    protected static RPCHook rpcHook;

    static {
        if (aclEnable) {
            log.info("acl enable");
//            rpcHook = AclClient.getAclRPCHook(ak, sk);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                MQAdmin.mqAdminExt.shutdown();
                log.info("Shutdown Hook is running !");
            }
        });
    }

    //    //
//    protected static Boolean fetchTopic(String instanceId, String messageType, String topic, Boolean needClean) {
//        log.info("[Topic] instanceId:{}, messageType:{}, topicName:{}", instanceId, messageType, topic);
//
//        boolean useExist = true;
//        if (topicDTO != null && topicDTO.getData() == null) {
//            log.info(String.format("start create topic: %s ", topic));
//            if (needClean) {
//                topicWrapper.createTopic(primaryAccount1, account1InstanceId, topic, messageType, "auto-test", true);
//            } else {
//                topicWrapper.createTopicEx(primaryAccount1, account1InstanceId, topic, messageType, "auto-test", true);
//            }
//            topicWrapper.waitTopicExist(primaryAccount1, account1InstanceId, topic);
//            TestUtils.waitForSeconds(42);
//            useExist = false;
//        } else {
//            log.info(String.format("topic %s has already been created", topic));
//        }
//        return useExist;
//    }
//
    protected static String getTopic(String messageType, String methodName) {
        String topic = String.format("topic_%s_%s_%s", messageType, methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[Topic] topic:{}, messageType:{}, methodName:{}", topic, messageType, methodName);
        boolean result = MQAdmin.createTopic(cluster, topic, 8, messageType);
        Assertions.assertTrue(result, String.format("create topic:%s failed", topic));
        return topic;
    }

    //    /**
//     * will delete topic
//     *
//     * @param instanceId
//     * @param messageType
//     * @param topic
//     * @return
//     */
//    protected static void getTopicRandom(String instanceId, String messageType, String topic) {
//        log.info("[Topic] instanceId:{}, messageType:{}, topic:{}", instanceId, messageType, topic);
//        GetTopicResponseBody topicDTO = topicWrapper.getTopic(primaryAccount1, instanceId, topic, true);
//        if (topicDTO.getData() == null) {
//            log.info(String.format("start create topic: %s ", topic));
//            topicWrapper.createTopic(primaryAccount1, account1InstanceId, topic, messageType, "auto-test", true);
//            topicWrapper.waitTopicExist(primaryAccount1, account1InstanceId, topic);
//        } else {
//            log.info(String.format("topic %s has already been created", topic));
//        }
//    }
//
    //The synchronization consumption retry policy is DefaultRetryPolicy
    protected static String getGroupId(String methodName) {
        String groupId = String.format("GID_%s_%s", methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[ConsumerGroupId] groupId:{}, methodName:{}", groupId, methodName);
        return groupId;
    }

    //
//    //The sequential consumption retry policy is FixedRetryPolicy
    protected static String getOrderlyGroupId(String methodName) {
        String groupId = String.format("GID_%s_%s", methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[ConsumerGroupId] groupId:{} methodName:{}", groupId, methodName);
        MQAdmin.createConsumerGroup(cluster, groupId, 30);
        return groupId;
    }

}
