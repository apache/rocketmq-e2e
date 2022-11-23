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

package org.apache.rocketmq.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQAdmin {
    private static final Logger log = LoggerFactory.getLogger(MQAdmin.class);
    public static DefaultMQAdminExt mqAdminExt;

    public static DefaultMQAdminExt initMQAdminExtInstance(String namesrvAddr) {
        mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setInstanceName(UUID.randomUUID().toString());
        mqAdminExt.setNamesrvAddr(namesrvAddr);
        try {
            mqAdminExt.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return mqAdminExt;
    }

    public static boolean createTopic(String clusterName, String topic, int queueNum) {
        int defaultWaitTime = 30;
        Map<String, String> attributes = new HashMap<>();
        return createTopic(clusterName, topic, queueNum, attributes, defaultWaitTime);
    }

    public static boolean createTopic(String clusterName, String topic, int queueNum, String type) {
        int defaultWaitTime = 30;
        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), type);
        return createTopic(clusterName, topic, queueNum, attributes, defaultWaitTime);
    }

    public static boolean createTopic(String clusterName, String topic, int queueNum, Map<String, String> attributes,
        int waitTimeSec) {
        boolean createResult = false;
        try {
            mqAdminExt.createTopic(clusterName, topic, queueNum, attributes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long startTime = System.currentTimeMillis();
        while (!createResult) {
            createResult = checkTopicExist(mqAdminExt, topic);
            if (System.currentTimeMillis() - startTime < waitTimeSec * 1000) {
                TestUtils.waitForMoment(100);
            } else {
                log.error(String.format("timeout,but create topic[%s] failed!", topic));
                break;
            }
        }
        if (createResult) {
            log.info("create topic:{} success", topic);
        }
        return createResult;
    }

    public static boolean createConsumerGroup(String clusterName, String groupId, int waitTimeSec) {
        boolean createResult = false;
        try {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setConsumeBroadcastEnable(false);
            subscriptionGroupConfig.setConsumeFromMinEnable(false);
            subscriptionGroupConfig.setGroupName(groupId);
            subscriptionGroupConfig.setConsumeMessageOrderly(true);

            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
            for (String addr : masterSet) {
                mqAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                log.info(String.format("create subscription group %s to %s success.\n", groupId, addr));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(String.format("create groupId:%s failed", groupId));
        }
//        long startTime = System.currentTimeMillis();
//        while (!createResult) {
//            createResult = checkConsumerGroupExist(mqAdminExt, groupId);
//            if (System.currentTimeMillis() - startTime < waitTimeSec * 1000) {
//                TestUtils.waitForMoment(100);
//            } else {
//                log.error(String.format("timeout,but create consumeGroup[%s] failed!", groupId));
//                break;
//            }
//        }
//        if (createResult) {
//            log.info("create consumeGroup:{} success", groupId);
//        }
        return createResult;
    }

    private static boolean checkTopicExist(DefaultMQAdminExt mqAdminExt, String topic) {
        boolean createResult = false;
        try {
            TopicStatsTable topicInfo = mqAdminExt.examineTopicStats(topic);
            createResult = !topicInfo.getOffsetTable().isEmpty();
        } catch (Exception e) {
        }
        return createResult;
    }

    private static boolean checkConsumerGroupExist(DefaultMQAdminExt mqAdminExt, String consumerGroup) {
        boolean createResult = false;
        try {
            ConsumeStats consumeStats = mqAdminExt.examineConsumeStats(consumerGroup);
            createResult = !consumeStats.getOffsetTable().isEmpty();
        } catch (Exception e) {
        }
        return createResult;
    }

    public static boolean createSub(String nameSrvAddr, String clusterName, String consumerId) {
        boolean createResult = true;
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(consumerId);
        try {
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
            for (String addr : masterSet) {
                try {
                    mqAdminExt.createAndUpdateSubscriptionGroupConfig(addr, config);
                    log.info(String.format("create subscription group %s to %s success.\n", consumerId, addr));
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000 * 1);
                }
            }
        } catch (Exception e) {
            createResult = false;
            e.printStackTrace();
        }
        return createResult;
    }

    public static ClusterInfo getCluster(String nameSrvAddr) {
        ClusterInfo clusterInfo = null;
        try {
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return clusterInfo;
    }

    public static boolean isBrokerExist(String ns, String ip) {
        ClusterInfo clusterInfo = getCluster(ns);
        if (clusterInfo == null) {
            return false;
        } else {
            Map<String, BrokerData> brokers = clusterInfo.getBrokerAddrTable();
            for (Entry<String, BrokerData> brokerEntry : brokers.entrySet()) {
                HashMap<Long, String> brokerIps = brokerEntry.getValue().getBrokerAddrs();
                for (Entry<Long, String> brokerIdEntry : brokerIps.entrySet()) {
                    if (brokerIdEntry.getValue().contains(ip)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public static void deleteTopic(String nameSrvAddr, String cluster, String topic) {
        try {
            Set<String> set = new HashSet<>();
            set.add(nameSrvAddr);
            mqAdminExt.deleteTopicInNameServer(set, topic);

            boolean isTopicExist = checkTopicExist(mqAdminExt, topic);
            long startTime = System.currentTimeMillis();
            while (!isTopicExist) {
                isTopicExist = checkTopicExist(mqAdminExt, topic);
                if (System.currentTimeMillis() - startTime < 5 * 1000) {
                    TestUtils.waitForMoment(100);
                } else {
                    log.error(String.format("timeout,but delete topic[%s] failed!", topic));
                    break;
                }
            }

        } catch (Exception e) {
        }
    }

    public void getSubConnection(String nameSrvAddr, String clusterName, String consumerId) {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(consumerId);
        try {
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
            for (String addr : masterSet) {
                try {

                    System.out.printf("create subscription group %s to %s success.\n", consumerId, addr);
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000 * 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
