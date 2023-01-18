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

package org.apache.rocketmq.utils;

import org.apache.rocketmq.client.rmq.DelayConf;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.utils.data.collect.DataCollector;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VerifyUtils {
    private static Logger logger = LoggerFactory.getLogger(VerifyUtils.class);
    private static final int TIMEOUT = 60;

    /**
     * 检验顺序消息
     * 校验点：1. 每个shardingkey中的消息都是顺序的
     *
     * @param receivedMessage
     * @return
     */
    public static boolean checkOrderMessage(ConcurrentHashMap<String, LinkedList<MessageExt>> receivedMessage) {
        for (Map.Entry<String, LinkedList<MessageExt>> stringLinkedListEntry : receivedMessage.entrySet()) {
            StringBuilder sb = new StringBuilder(String.format("shardingKey %s,message order: ", stringLinkedListEntry.getKey()));
            int preNode = -1;
            LinkedList<MessageExt> messages = stringLinkedListEntry.getValue();
            String tag = messages.getFirst().getTags();
            for (MessageExt message : messages) {
                if (!message.getTags().equals(tag)) {
                    preNode = -1;
                }
                int curNode = Integer.parseInt(new String(message.getBody()));
                sb.append(curNode).append(",");
                if (preNode > curNode) {
                    logger.error(sb.toString());
                    return false;
                }
                preNode = curNode;
            }
        }
        return true;
    }

    /**
     * 普通消息检查器
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     */
    public static void verifyNormalMessage(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * 普通消息检查器
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     */
    public static void verifyNormalMessage(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, int timeout) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, timeout * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * 普通消息检查器，订阅消息是否与发送消息内容一致
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     * @param messageBody     消息体
     */
    public static void verifyNormalMessageWithBody(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, String messageBody) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        Collection<MessageExt> receivedMessages = dequeueMessages.getAllData();
        List<Message> messages = new ArrayList<>(receivedMessages);
        for (Message message : messages) {
            Assertions.assertEquals(messageBody, new String(message.getBody()), "订阅到的messageBody与期望不符");
        }
    }

    /**
     * 顺序消息检查器
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     */
    public static void verifyOrderMessage(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages) {
        //检查是否消费完成
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //检查是否消费顺序性             logger.warn(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        Assertions.assertTrue(checkOrder(dequeueMessages), "消息非顺序");
    }

    /**
     * 延迟、定时消息检查器
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     * @param delayLevel      预计需要的消费时间
     */
    public static void verifyDelayMessage(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, int delayLevel) {
        //检查是否消费完成
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, (TIMEOUT + DelayConf.DELAY_LEVEL[delayLevel - 1]) * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //检查是否消费延迟性
        HashMap<String, Long> delayUnExcept = checkDelay(dequeueMessages, 5);
        StringBuilder sb = new StringBuilder();
        sb.append("以下消息不符合延迟要求 \n");
        for (String msg : delayUnExcept.keySet()) {
            sb.append(msg).append(" , interval:").append(delayUnExcept.get(msg)).append("\n");
        }
        Assertions.assertEquals(0, delayUnExcept.size(), sb.toString());
    }

    /**
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     * @param delayTime       延迟时间
     * @param count           不被消费的数量
     */
    public static void verifyDelayMessageWithUnConsumeCount(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, int delayTime, int count) {
        //检查是否消费完成
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, (TIMEOUT + delayTime) * 1000L, 1);
        if (unConsumedMessages.size() > count) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //检查是否消费延迟性
        HashMap<String, Long> delayUnExcept = checkDelay(dequeueMessages, TIMEOUT + 5);
        StringBuilder sb = new StringBuilder();
        sb.append("以下消息不符合延迟要求 \n");
        //时间戳格式化
        SimpleDateFormat date = new SimpleDateFormat("ss");
        for (String msg : delayUnExcept.keySet()) {
            sb.append(msg).append(" , interval:").append("相差" + date.format(new Date(Long.parseLong(String.valueOf(delayUnExcept.get(msg))))) + "秒").append("\n");
        }
        Assertions.assertEquals(0, delayUnExcept.size(), sb.toString());
    }

    /**
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     * @param delayTime       延迟时间
     * @param reconsumeTime   重试次数
     */
    public static void verifyDelayMessageWithReconsumeTimes(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, int delayTime, int reconsumeTime) {
        int flexibleTime = TIMEOUT;
        if (reconsumeTime == 1) {
            flexibleTime = flexibleTime + 10;
        } else if (reconsumeTime == 2) {
            flexibleTime = flexibleTime + 10 + 30;
        } else if (reconsumeTime == 3) {
            flexibleTime = flexibleTime + 10 + 30 + 60;
        } else if (reconsumeTime == 4) {
            flexibleTime = flexibleTime + 10 + 30 + 60 + 120;
        }
        //检查是否消费完成
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, (flexibleTime + delayTime) * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //检查是否消费延迟性
        HashMap<String, Long> delayUnExcept = checkDelay(dequeueMessages, 5 + flexibleTime - 30);
        StringBuilder sb = new StringBuilder();
        sb.append("以下消息不符合延迟要求 \n");
        for (String msg : delayUnExcept.keySet()) {
            sb.append(msg).append(" , interval:").append(delayUnExcept.get(msg)).append("\n");
        }
        Assertions.assertEquals(0, delayUnExcept.size(), sb.toString());
    }

    public static void verifyNormalMessageWithReconsumeTimes(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, int reconsumeTime) {
        int flexibleTime = TIMEOUT;
        if (reconsumeTime == 1) {
            flexibleTime = flexibleTime + 10;
        } else if (reconsumeTime == 2) {
            flexibleTime = flexibleTime + 10 + 30;
        } else if (reconsumeTime == 3) {
            flexibleTime = flexibleTime + 10 + 30 + 60;
        } else if (reconsumeTime == 4) {
            flexibleTime = flexibleTime + 10 + 30 + 60 + 120;
        }
        //检查是否消费完成
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, flexibleTime * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * 校验消息重试消费
     *
     * @param enqueueMessages    发送的消息列表
     * @param dequeueAllMessages 消费的消息列表
     * @param consumedTimes      重复消费的次数
     */
    public static void verifyRetryConsume(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueAllMessages, int consumedTimes) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueAllMessages, TIMEOUT * 1000L, consumedTimes);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * 事务消息检查器
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     */
    public static void checkTransactionMessage(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    public static void verifyConsumeFailed(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, Integer reconsumeTimes) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, reconsumeTimes + 1);
        if (unConsumedMessages.size() > 0) {
            //Assertions.fail(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
            logger.warn(String.format("以下%s条消息未被消费: %s", unConsumedMessages.size(), unConsumedMessages));
        } else {
            Assertions.fail("消息全部被消费");
        }
    }

    /**
     * @param everyConsumeResult
     * @param batchConsumeSize
     * @param lastConsumeSize
     */
    public static void verifyBatchSize(List<Integer> everyConsumeResult, int batchConsumeSize, int lastConsumeSize) {
        boolean result = false;
        if (lastConsumeSize == 0) {
            long s = everyConsumeResult.stream().filter(a -> a == batchConsumeSize).count();
            if (s == everyConsumeResult.size()) {
                result = true;
            }
        } else {
            if (lastConsumeSize == everyConsumeResult.get(everyConsumeResult.size() - 1)) {
                everyConsumeResult.remove(everyConsumeResult.size() - 1);
                long s = everyConsumeResult.stream().filter(a -> a == batchConsumeSize).count();
                if (s == everyConsumeResult.size()) {
                    result = true;
                }
            }
        }
        Assertions.assertTrue(result, "批量消费校验失败");
    }

    /**
     * 校验负载均衡
     *
     * @param msgSize  消费到的消息条数
     * @param recvSize 每个客户端消费到的消息条数
     */
    public static void verifyBalance(int msgSize, long... recvSize) {
        Assertions.assertTrue(verifyBalance(msgSize, 0.1f, recvSize), "客户端负载不均衡 " + Arrays.toString(recvSize));
        //return verifyBalance(msgSize, 0.1f, recvSize);
    }

    private static boolean verifyBalance(int msgSize, float error, long... recvSize) {
        boolean balance = true;
        int evenSize = msgSize / recvSize.length; //平均值
        for (long size : recvSize) {
            //如果消费到的消息比平均值大于误差则不算均衡
            if (Math.abs(size - evenSize) > error * evenSize) {
                balance = false;
                logger.error("msgSize:{}, recvSize:{}, not balance!", msgSize, recvSize);
                break;
            }
        }
        return balance;
    }

    private static HashMap<String, Long> checkDelay(DataCollector<MessageExt> dequeueMessages, int offset) {
        HashMap<String, Long> map = new HashMap<>();
        Collection<MessageExt> receivedMessages = dequeueMessages.getAllData();
        for (MessageExt receivedMessage : receivedMessages) {
            long startDeliverTime = Long.parseLong(receivedMessage.getUserProperty("startDeliverTime"));
            //判断当前时间跟分发时间，如果相差在5s内，则满足要求
            long bornTimestamp = receivedMessage.getBornTimestamp();
            //if ()
            if (Math.abs(startDeliverTime - bornTimestamp) / 1000 > DelayConf.DELAY_LEVEL[receivedMessage.getDelayTimeLevel() - 1] + offset) {
                map.put(receivedMessage.getMsgId(), (startDeliverTime - bornTimestamp) / 1000);
            }
        }
        return map;
    }

    /**
     * 检查消息的顺序性
     *
     * @param dequeueMessages 收到的消息集合
     * @return 是否分区顺序
     */
    private static boolean checkOrder(DataCollector<MessageExt> dequeueMessages) {
        Collection<MessageExt> receivedMessages = dequeueMessages.getAllData();
        ConcurrentHashMap<String, LinkedList<MessageExt>> map = new ConcurrentHashMap<>();
        for (MessageExt receivedMessage : receivedMessages) {
            String shardingKey = String.valueOf(receivedMessage.getQueueId());
            LinkedList<MessageExt> messages;
            if (map.containsKey(shardingKey)) {
                messages = map.get(shardingKey);
                messages.add(receivedMessage);
            } else {
                messages = new LinkedList<>();
                messages.add(receivedMessage);
                map.put(shardingKey, messages);
            }
        }
        return checkOrderMessage(map);
    }

    /**
     * 检查发送的消息是否都被消费
     *
     * @param enqueueMessages 发送入队消息集合
     * @param dequeueMessages 消费出队消息集合
     * @param timeoutMills    检查超时时间
     * @param consumedTimes
     * @return 未被消费的消息集合
     */
    private static Collection<MessageExt> waitForMessageConsume(DataCollector<MessageExt> enqueueMessages, DataCollector<MessageExt> dequeueMessages, Long timeoutMills, Integer consumedTimes) {
        logger.info("Set timeout: {}ms", timeoutMills);
        Collection<MessageExt> sendMessages = enqueueMessages.getAllData();

        long currentTime = System.currentTimeMillis();

        while (!sendMessages.isEmpty()) {
            //logger.info("param1:{}, param2:{}", enqueueMessages.getDataSize(), dequeueMessages.getDataSize());
            List<MessageExt> receivedMessagesCopy = new ArrayList<>(dequeueMessages.getAllData());
            Iterator<MessageExt> iter = sendMessages.iterator();
            while (iter.hasNext()) {
                MessageExt message = iter.next();
                long msgCount = receivedMessagesCopy
                        .stream()
                        .filter(msg -> {
                            if (msg.getUserProperty("UNIQ_KEY") != null && !msg.getUserProperty("UNIQ_KEY").equals(msg.getMsgId())) {
                                return msg.getUserProperty("UNIQ_KEY").equals(message.getMsgId()) || msg.getMsgId().equals(message.getMsgId());
                            }
                            return msg.getMsgId().equals(message.getMsgId());
                        })
                        .count();
                if (msgCount > 0 && getRepeatedTimes(receivedMessagesCopy, message) == consumedTimes) {
                    iter.remove();
                } else if (getRepeatedTimes(receivedMessagesCopy, message) > consumedTimes) {
                    Assertions.fail(String.format("消费到的重试消息多于预期（包含一条原始消息），Except:%s, Actual:%s, MsgId:%s", consumedTimes, getRepeatedTimes(receivedMessagesCopy, message), message.getMsgId()));
                    //logger.error("消费到的重试消息多于预期，Except:{}, Actual:{}", consumedTimes, getRepeatedTimes(receivedMessagesCopy, message));
                }
            }
            if (sendMessages.isEmpty()) {
                break;
            }
            if (System.currentTimeMillis() - currentTime >= timeoutMills) {
                logger.error("Timeout but not received all send messages, not received msg: {}\n received msg:{}\n", sendMessages, receivedMessagesCopy);
                break;
            }
            TestUtils.waitForMoment(500L);
        }
        return sendMessages;
    }

    private static synchronized int getRepeatedTimes(Collection<MessageExt> recvMsgs, MessageExt msg) {
        int count = 0;
        for (MessageExt recvMsg : recvMsgs) {
            if (recvMsg.getUserProperty("UNIQ_KEY") != null && !recvMsg.getUserProperty("UNIQ_KEY").equals(recvMsg.getMsgId()) && !recvMsg.getMsgId().equals(msg.getMsgId())) {
                if (recvMsg.getUserProperty("UNIQ_KEY").equals(msg.getMsgId())) {
                    count++;
                }
            } else if (recvMsg.getMsgId().equals(msg.getMsgId())) {
                count++;
            }
        }
        return count;
    }

    /**
     * 校验一段时间内没有消费
     *
     * @param receivedMessages
     * @param timeout          时间
     */
    public static void waitForConsumeFailed(DataCollector<MessageExt> receivedMessages, int timeout) {
        long currentTime = System.currentTimeMillis();
        while (currentTime + timeout * 1000L > System.currentTimeMillis()) {
            if (receivedMessages.getDataSize() > 0) {
                Assertions.fail("消费到消息");
                break;
            }
            TestUtils.waitForSeconds(5);
        }
    }
}
