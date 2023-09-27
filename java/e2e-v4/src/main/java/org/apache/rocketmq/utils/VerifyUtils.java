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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.rmq.DelayConf;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.utils.data.collect.DataCollector;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class VerifyUtils {
    private static Logger logger = LoggerFactory.getLogger(VerifyUtils.class);
    private static AtomicInteger receivedIndex = new AtomicInteger(0);
    private static final int TIMEOUT = 60;
    private static int defaultSimpleThreadNums = 4;

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

    public static void verifyNormalMessage(DataCollector<MessageExt> enqueueMessages,
                                           DataCollector<MessageExt> dequeueMessages, Set<String> unconsumedMsgIds, int timeout) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, timeout * 1000L, 1);
        Set<MessageExt> unConsumedMessagesCopy = new HashSet<>(unConsumedMessages);
        System.out.println(unConsumedMessagesCopy.size());
        Set<String> finalUnconsumedMsgIds = unconsumedMsgIds;
        unConsumedMessagesCopy = unConsumedMessagesCopy.stream().filter(msgExt-> !finalUnconsumedMsgIds.contains(msgExt.getMsgId())).collect(Collectors.toSet());
        System.out.println(unConsumedMessagesCopy.size());
        StringBuilder sb = new StringBuilder();
        boolean allInUnConsumedMessages = true;
        for(String unconsumedMsgId:unconsumedMsgIds){
            boolean check = false;
            for(MessageExt unConsumedMessage : unConsumedMessages){
                if (unConsumedMessage.getMsgId().equals(unconsumedMsgId)){
                    check = true;
                }
            }
            if (!check){
                allInUnConsumedMessages = false;
                break;
            }
        }
        if (!allInUnConsumedMessages) {
            unconsumedMsgIds = unconsumedMsgIds.stream().filter(msgId -> {
                for(MessageExt unConsumedMessage:unConsumedMessages){
                    if (unConsumedMessage.getMsgId().equals(msgId)){
                        return false;
                    }
                }
                return true;
            }).collect(Collectors.toSet());
            logger.info(unconsumedMsgIds.size() + "messages are consumed:" + unconsumedMsgIds.size());
            sb.append("The following ").append(unconsumedMsgIds.size()).append(" messages are consumed:").append(unconsumedMsgIds);
            Assertions.fail(sb.toString());
        }
        if (unConsumedMessagesCopy.size() > 0) {
            logger.info(unConsumedMessagesCopy.size() + "messages are not consumed:" + unConsumedMessagesCopy);
            MessageExt messageExt = dequeueMessages.getFirstElement();
            sb.append(messageExt.getTopic()).append(" The following").append(unConsumedMessagesCopy.size()).append("messages are not consumed:").append(unConsumedMessagesCopy);
            Assertions.fail(sb.toString());
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

    public static void tryReceiveOnce(DefaultLitePullConsumer consumer) {
        tryReceiveOnce(consumer, false, false);
    }

    public static void tryReceiveOnce(DefaultMQPullConsumer consumer, String topic, String tag, int maxNums) {
        tryReceiveOnce(consumer,topic,tag,maxNums,false,false);
    }

    public static void tryReceiveOnce(DefaultMQPullConsumer consumer,String topic, String tag,int maxNums, Boolean useExistTopic, Boolean useExistGid) {
        Set<MessageQueue> messageQueues = null;
        try {
            messageQueues = consumer.fetchSubscribeMessageQueues(topic);
        } catch (MQClientException e) {
            Assertions.fail("Fail to fetchSubscribeMessageQueues");
        }

        long start = System.currentTimeMillis();
        if (!useExistTopic || !useExistGid) {
            for (int i = 0; i < 5; i++) {
                logger.info("Try pulling a message once");
                Set<MessageQueue> finalMessageQueues = messageQueues;
                CompletableFuture[] futures = new CompletableFuture[messageQueues.size()];
                int mqCount = 0;
                for (MessageQueue mq : finalMessageQueues) {
                    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            long offset = consumer.fetchConsumeOffset(mq, false);
                            if (offset< 0 ) return null;
                            boolean shouldContinue = true;
                            while (shouldContinue) {
                                PullResult pullResult = consumer.pull(mq, tag, offset, maxNums);
                                switch (pullResult.getPullStatus()) {
                                    case FOUND:
                                        List<MessageExt> messages = pullResult.getMsgFoundList();
                                        for (MessageExt message : messages) {
                                            receivedIndex.getAndIncrement();
                                            logger.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                                    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())), message.getProperties(), message.getReconsumeTimes());
                                        }
                                        offset = pullResult.getNextBeginOffset();
                                        consumer.updateConsumeOffset(mq, offset);
                                        break;
                                    case NO_MATCHED_MSG:
                                        shouldContinue = false; // 当没有匹配的消息时退出循环
                                        break;
                                    case NO_NEW_MSG:
                                        shouldContinue = false; // 当没有新的消息时退出循环
                                        break;
                                    case OFFSET_ILLEGAL:
                                        shouldContinue = false; // 当偏移量非法时退出循环
                                        break;
                                    default:
                                        break;
                                }
                            }
                        } catch (MQBrokerException e) {
                            e.printStackTrace();
                            Assertions.fail("Pull fetch message error");
                        } catch (RemotingException e) {
                            e.printStackTrace();
                            Assertions.fail("Pull fetch message error");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            Assertions.fail("Pull fetch message error");
                        } catch (MQClientException e) {
                            e.printStackTrace();
                            Assertions.fail("Pull fetch message error");
                        }
                        return null;
                    });
                    futures[mqCount++] = future;
                }
                try {
                    CompletableFuture.allOf(futures).get(6, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assertions.fail("receive response count not match");
                }
            }
        }
        logger.info("receive server response, cost={}ms", System.currentTimeMillis() - start);
    }

    public static void tryReceiveOnce(DefaultLitePullConsumer consumer, Boolean useExistTopic, Boolean useExistGid) {
        long start = System.currentTimeMillis();
        if (!useExistTopic || !useExistGid) {
            CompletableFuture[] cfs = new CompletableFuture[5];
            for (int i = 0; i < 5; i++) {
                logger.info("Try pulling a message once");
                int finalI = i;
                CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> consumer.poll(2000))
                        .thenAcceptAsync(extList -> {
                            if (extList.size() > 0) {
                                for (MessageExt ext : extList) {
                                    receivedIndex.getAndIncrement();
                                    logger.info("MessageId:{}, Body:{}, Property:{}, Index:{}, Retry:{}", ext.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ext.getBody())), ext.getProperties(), finalI, ext.getReconsumeTimes());
                                }
                            }
                        });
                cfs[i] = future;
            }
            try {
                CompletableFuture.allOf(cfs).get(30, TimeUnit.SECONDS);
                logger.info("receive server response, cost={}ms", System.currentTimeMillis() - start);
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail("receive response count not match");
            }
        }
    }

    private static synchronized int getRepeatedTimes(Collection<MessageExt> recvMsgs, String enqueueMessageId) {
        int count = 0;
        for (MessageExt recvMsg : recvMsgs) {
            if (recvMsg.getMsgId().equals(enqueueMessageId)) {
                count++;
            }
        }
        return count;
    }


    /**
     * Verifying Cluster Consumption
     *
     * @param enqueueMessages    All messages sent
     * @param dequeueAllMessages Multiple consumer end, consumption of all messages
     */
    @SafeVarargs
    public static void verifyClusterConsume(DataCollector<MessageExt> enqueueMessages,
                                            DataCollector<MessageExt>... dequeueAllMessages) {
        long currentTime = System.currentTimeMillis();
        List<MessageExt> sendMessagesCopy = new ArrayList<>(enqueueMessages.getAllData());

        while (!sendMessagesCopy.isEmpty()) {
            Collection<MessageExt> noDupMsgs = new ArrayList<>();
            for (DataCollector<MessageExt> messages : dequeueAllMessages) {
                noDupMsgs.addAll(messages.getAllData());
                logger.info("consumer received message: {}", messages.getDataSize());
            }
            logger.info("sendMessagesCopy left: {}", sendMessagesCopy.size());

            List<MessageExt> receivedMessagesCopy = new ArrayList<>(noDupMsgs);
            Iterator<MessageExt> iter = sendMessagesCopy.iterator();
            while (iter.hasNext()) {
                MessageExt messageExt = iter.next();
                String messageId = messageExt.getMsgId();
                long msgCount = receivedMessagesCopy.stream().filter(msg -> msg.getMsgId().equals(messageId)).count();
                if (msgCount > 0 && getRepeatedTimes(receivedMessagesCopy, messageId) == 1) {
                    iter.remove();
                }
            }
            if (sendMessagesCopy.isEmpty()) {
                break;
            }
            if (System.currentTimeMillis() - currentTime >= 60000L) {
                logger.error("Timeout but not received all send messages, not received msg: {}\n received msg:{}\n", sendMessagesCopy, receivedMessagesCopy);
                break;
            }
            TestUtils.waitForMoment(500L);
        }

        Assertions.assertEquals(0, sendMessagesCopy.size(), String.format("The following %s messages are not consumed: %s", sendMessagesCopy.size(), sendMessagesCopy));

    }

    /**
     * Validation of sql attribute filtering
     *
     * @param enqueueMessages A message sent
     * @param dequeueMessages News of consumption
     * @param props           The desired attribute condition is not met
     */
    public static void verifyNormalMessageWithUserProperties(DataCollector<MessageExt> enqueueMessages,
                                                             DataCollector<MessageExt> dequeueMessages, HashMap<String, String> props, int expectedUnrecvMsgNum) {
        Collection<MessageExt> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        Collection<MessageExt> recvMsgs = dequeueMessages.getAllData();
        for (MessageExt unConsumedMessage : recvMsgs) {
            for (Map.Entry<String, String> entry : props.entrySet()) {
                Map<String, String> msgProperties = unConsumedMessage.getProperties();
                for (Map.Entry<String, String> property : msgProperties.entrySet()) {
                    if (property.getKey().equals(entry.getKey()) && property.getValue().equals(entry.getValue())) {
                        Assertions.fail("sql attribute filtering is not in effect, consuming messages to other attributes," + unConsumedMessage.getProperties().toString());
                    }
                }
            }
        }
        if (unConsumedMessages.size() != expectedUnrecvMsgNum) {
            Assertions.fail("Failed to consume all the sent data by sql filter");
        }
    }

    public static void waitLitePullReceiveThenAck(RMQNormalProducer producer, DefaultLitePullConsumer consumer,String topic,String tag) {
        Assertions.assertFalse(consumer.isAutoCommit());
        ArrayList<MessageQueue>  assignList = null;
        try {
            assignList = new ArrayList<>(consumer.fetchMessageQueues(topic));
        } catch (MQClientException e) {
            Assertions.fail("PullConsumer fetchMessageQueues error");
        }
        Assertions.assertNotNull(assignList);
        consumer.assign(assignList);

        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<MessageExt> sendCollection = producer.getEnqueueMessages().getAllData();
        try {
            while (endTime > System.currentTimeMillis()) {
                final List<MessageExt> extList = consumer.poll();
                if (extList.size() > 0) {
                    for (MessageExt messageExt : extList) {
                        receivedIndex.getAndIncrement();
                        String tags = messageExt.getTags();
                        FilterUtils.inTags(tags, tag);
                        logger.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}", messageExt.getMsgId(),
                                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(messageExt.getBody())), messageExt.getTags(), messageExt.getProperties(), receivedIndex.get());
                        sendCollection.removeIf(sendMessageExt -> sendMessageExt.getMsgId().equals(messageExt.getMsgId()));
                    }
                }
                consumer.commitSync();
                logger.info("Pull message: {} bar, remaining unconsumed message: {} bar", extList.size(), sendCollection.size());
                if (sendCollection.size() == 0) {
                    break;
                }
            }
            Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    public static void waitPullReceiveThenAck(RMQNormalProducer producer, DefaultMQPullConsumer consumer, String topic, String tag, int maxNums) {
        Set<MessageQueue> messageQueues = null;
        try {
            messageQueues = consumer.fetchSubscribeMessageQueues(topic);
        } catch (MQClientException e) {
            Assertions.fail("Fail to fetchSubscribeMessageQueues");
        }

//        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<MessageExt> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());
        Set<MessageQueue> finalMessageQueues = messageQueues;
        CompletableFuture[] futures = new CompletableFuture[messageQueues.size()];
        int mqCount = 0;
        for (MessageQueue mq : finalMessageQueues) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset< 0 ) return null;
                    boolean shouldContinue = true;
                    while (shouldContinue) {
                        PullResult pullResult = consumer.pull(mq, tag, offset, maxNums);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> messages = pullResult.getMsgFoundList();
                                for (MessageExt message : messages) {
                                    receivedIndex.getAndIncrement();
                                    logger.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())), message.getProperties(), message.getReconsumeTimes());
                                    offset = message.getQueueOffset()+1;
                                    consumer.updateConsumeOffset(mq, offset);
                                    sendCollection.removeIf(messageExt -> messageExt.getMsgId().equals(message.getMsgId()));
                                }
                                break;
                            case NO_MATCHED_MSG:
                                shouldContinue = false; // 当没有匹配的消息时退出循环
                                break;
                            case NO_NEW_MSG:
                                shouldContinue = false; // 当没有新的消息时退出循环
                                break;
                            case OFFSET_ILLEGAL:
                                shouldContinue = false; // 当偏移量非法时退出循环
                                break;
                            default:
                                break;
                        }
                    }
                } catch (MQBrokerException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (RemotingException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (MQClientException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                }
                return null;
            });
            futures[mqCount++] = future;
        }
        try {
            CompletableFuture.allOf(futures).get(6, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("receive response count not match");
        }
    }

    public static void waitFIFOReceiveThenAck(RMQNormalProducer producer, DefaultMQPullConsumer consumer, String topic, String tag, int maxNums) {
        Set<MessageQueue> messageQueues = null;
        try {
            messageQueues = consumer.fetchSubscribeMessageQueues(topic);
        } catch (MQClientException e) {
            Assertions.fail("Fail to fetchSubscribeMessageQueues");
        }

        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<MessageExt> sendCollection = producer.getEnqueueMessages().getAllData();
        ConcurrentHashMap<String, LinkedList<MessageExt>> map = new ConcurrentHashMap<>();

        Set<MessageQueue> finalMessageQueues = messageQueues;
        CompletableFuture[] futures = new CompletableFuture[messageQueues.size()];
        int mqCount = 0;
        for (MessageQueue mq : finalMessageQueues) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset< 0 ) return null;
                    boolean shouldContinue = true;
                    while (shouldContinue) {
                        PullResult pullResult = consumer.pull(mq, tag, offset, maxNums);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> messages = pullResult.getMsgFoundList();
                                for (MessageExt message : messages) {
                                    receivedIndex.getAndIncrement();
                                    logger.info("MessageId:{}, Body:{}, Property:{}, Retry:{}", message.getMsgId(),
                                            StandardCharsets.UTF_8.decode(ByteBuffer.wrap(message.getBody())), message.getProperties(), message.getReconsumeTimes());
                                    offset = message.getQueueOffset()+1;
                                    consumer.updateConsumeOffset(mq, offset);
                                    sendCollection.removeIf(messageExt -> messageExt.getMsgId().equals(message.getMsgId()));
                                    String shardingKey = String.valueOf(mq.getQueueId());
                                    LinkedList<MessageExt> messagesList;
                                    if (map.containsKey(shardingKey)) {
                                        messagesList = map.get(shardingKey);
                                        messagesList.add(message);
                                    } else {
                                        messagesList = new LinkedList<>();
                                        messagesList.add(message);
                                        map.put(shardingKey, messagesList);
                                    }
                                    if (sendCollection.size() == 0) {
                                        Assertions.assertTrue(checkOrderMessage(map), "Consumption is not sequential");
                                    }
                                }
                                break;
                            case NO_MATCHED_MSG:
                                shouldContinue = false; // 当没有匹配的消息时退出循环
                                break;
                            case NO_NEW_MSG:
                                shouldContinue = false; // 当没有新的消息时退出循环
                                break;
                            case OFFSET_ILLEGAL:
                                shouldContinue = false; // 当偏移量非法时退出循环
                                break;
                            default:
                                break;
                        }
                    }
                } catch (MQBrokerException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (RemotingException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                } catch (MQClientException e) {
                    e.printStackTrace();
                    Assertions.fail("Pull fetch message error");
                }
                return null;
            });
            futures[mqCount++] = future;
        }
        try {
            CompletableFuture.allOf(futures).get(6, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("receive response count not match");
        }
        Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
    }
}
