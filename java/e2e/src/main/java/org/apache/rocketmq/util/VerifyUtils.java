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
package org.apache.rocketmq.util;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.util.data.collect.DataCollector;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class VerifyUtils {
    private static final Logger log = LoggerFactory.getLogger(VerifyUtils.class);
    private static final int TIMEOUT = 90;
    private static AtomicInteger receivedIndex = new AtomicInteger(0);
    private static int defaultSimpleThreadNums = 4;

    /**
     * check order message
     * point: every shardingkey is orderly
     *
     * @param receivedMessage
     * @return
     */
    public static boolean checkOrderMessage(ConcurrentHashMap<String, LinkedList<Object>> receivedMessage) {
        for (Map.Entry<String, LinkedList<Object>> stringLinkedListEntry : receivedMessage.entrySet()) {
            StringBuilder sb = new StringBuilder(String.format("shardingKey %s,message order: ", stringLinkedListEntry.getKey()));
            int preNode = -1;
            LinkedList<Object> messages = stringLinkedListEntry.getValue();

            String tag = ((MessageView) messages.getFirst()).getTag().toString();
            for (Object message : messages) {
                MessageView messageView = (MessageView) message;
                if (!messageView.getTag().toString().equals(tag)) {
                    preNode = -1;
                }
                int curNode = Integer.parseInt(StandardCharsets.UTF_8.decode(messageView.getBody()).toString());
                sb.append(curNode).append(",");
                if (preNode > curNode) {
                    log.error(sb.toString());
                    return false;
                }
                preNode = curNode;
            }
        }
        return true;
    }

    /**
     * check normal message
     *
     * @param enqueueMessages
     * @param dequeueMessages
     */
    public static void verifyNormalMessage(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    public static void verifyNormalMessage(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, Set<String> unconsumedMsgIds, int timeout) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, timeout * 1000L, 1);
//        if (unConsumedMessages.size() == 0) {
//            Assertions.fail(String.format("Messages are all consumed"));
//        } else {
//            for (Object messageId : unConsumedMessages) {
//                unconsumedMsgIds.removeIf(msgId -> ((String)messageId).equals(msgId));
//            }
//        }
        Set<Object> unConsumedMessagesCopy = new HashSet<>(unConsumedMessages);
        unConsumedMessagesCopy.removeAll(unconsumedMsgIds);
        StringBuilder sb = new StringBuilder();
        if (!unConsumedMessages.containsAll(unconsumedMsgIds)) {
            unconsumedMsgIds.removeAll(unConsumedMessages);
            log.info(unconsumedMsgIds.size() + "messages are consumed:" + unconsumedMsgIds.size());
            sb.append("The following ").append(unconsumedMsgIds.size()).append(" messages are consumed:").append(unconsumedMsgIds);
            Assertions.fail(sb.toString());
        }
        if (unConsumedMessagesCopy.size() > 0) {
            log.info(unConsumedMessagesCopy.size() + "messages are not consumed:" + unConsumedMessagesCopy);
            MessageView messageView = (MessageView) dequeueMessages.getFirstElement();
            sb.append(messageView.getTopic()).append(" The following").append(unConsumedMessagesCopy.size()).append("messages are not consumed:").append(unConsumedMessagesCopy);
            Assertions.fail(sb.toString());
        }

    }

    public static void verifyNormalMessage(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, int timeout) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, timeout * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * General message checker, whether the subscribed message is the same as the sent message
     *
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     * @param messageBody     Body of message
     */
    public static void verifyNormalMessageWithBody(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, String messageBody) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        Collection<Object> receivedMessages = dequeueMessages.getAllData();
        List<Object> messages = new ArrayList<>(receivedMessages);
        for (Object message : messages) {
            MessageView messageView = (MessageView) message;
            Assertions.assertEquals(messageBody, StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), "The messageBody you subscribed to did not match your expectations");
        }
    }

    /**
     * Sequential message inspector
     *
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     */
    public static void verifyOrderMessage(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages) {
        //Check whether the consumption is complete
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("%s The following %s messages are not consumed: %s", unConsumedMessages.toArray()[0], unConsumedMessages.size(), unConsumedMessages));
        }
        //Check for sequential consumption             log.warn(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        Assertions.assertTrue(checkOrder(dequeueMessages), "Message out of order");
    }

    /**
     * Delayed and timed message checker
     *
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     * @param delayTime       Estimated consumption time required
     */
    public static void verifyDelayMessage(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, int delayTime) {
        //Check whether the consumption is complete
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages,
            (TIMEOUT + delayTime) * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //Check for consumption latency
        HashMap<String, Long> delayUnExcept = checkDelay(dequeueMessages, delayTime);
        StringBuilder sb = new StringBuilder();
        sb.append("The following messages do not meet the delay requirements \n");
        for (String msg : delayUnExcept.keySet()) {
            sb.append(msg).append(" , interval:").append(delayUnExcept.get(msg)).append("\n");
        }
        Assertions.assertEquals(0, delayUnExcept.size(), sb.toString());
    }

    /**
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     * @param delayTime       Time of delay
     * @param count           The amount that is not consumed
     */
    public static void verifyDelayMessageWithUnConsumeCount(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, int delayTime,
        int count) {
        //Check whether the consumption is complete
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, (TIMEOUT + delayTime) * 1000L, 1);
        if (unConsumedMessages.size() > count) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //Check for consumption latency
        HashMap<String, Long> delayUnExcept = checkDelay(dequeueMessages, delayTime);
        StringBuilder sb = new StringBuilder();
        sb.append("The following message does not meet the delay requirement \n");
        //Time stamp formatting
        SimpleDateFormat date = new SimpleDateFormat("ss");
        for (String msg : delayUnExcept.keySet()) {
            sb.append(msg).append(" , interval:").append("Difference between" + date.format(new Date(Long.parseLong(String.valueOf(delayUnExcept.get(msg))))) + "s").append(
                "\n");
        }
        Assertions.assertEquals(0, delayUnExcept.size(), sb.toString());
    }

    /**
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     * @param delayTime       Time of delay
     * @param reconsumeTime   Number of retries
     */
    public static void verifyDelayMessageWithReconsumeTimes(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, int delayTime,
        int reconsumeTime) {
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
        //Check whether the consumption is complete
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, (flexibleTime + delayTime) * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
        //Check for consumption latency
        HashMap<String, Long> delayUnExcept = checkDelay(dequeueMessages, delayTime);
        StringBuilder sb = new StringBuilder();
        sb.append("The following message does not meet the delay requirement \n");
        for (String msg : delayUnExcept.keySet()) {
            sb.append(msg).append(" , interval:").append(delayUnExcept.get(msg)).append("\n");
        }
        Assertions.assertEquals(0, delayUnExcept.size(), sb.toString());
    }

    public static void verifyNormalMessageWithReconsumeTimes(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages,
        int reconsumeTime) {
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
        //Check whether the consumption is complete
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, flexibleTime * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * Verify message retry consumption
     *
     * @param enqueueMessages    List of messages to send
     * @param dequeueAllMessages A list of messages to consume
     * @param consumedTimes      The number of repeated purchases
     */
    public static void verifyRetryConsume(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueAllMessages, int consumedTimes) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueAllMessages, TIMEOUT * 1000L, consumedTimes);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    /**
     * Transaction message inspector
     *
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     */
    public static void checkTransactionMessage(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    private static HashMap<String, Long> checkDelay(DataCollector<Object> dequeueMessages, int delayTimeSec) {
        HashMap<String, Long> map = new HashMap<>();
        Collection<Object> receivedMessages = dequeueMessages.getAllData();
        long consumeTime = System.currentTimeMillis();
        for (Object receivedMessage : receivedMessages) {
            MessageView messageView = (MessageView) receivedMessage;
            Assertions.assertTrue(messageView.getDeliveryTimestamp().isPresent(), "DeliveryTimestamp is empty");
            //Check the current time and the distribution time. If the difference is within 5s, the requirements are met
            long bornTimestamp = messageView.getBornTimestamp();
            if (Math.abs((consumeTime - bornTimestamp) / 1000 - delayTimeSec) > 5) {
                map.put(messageView.getMessageId().toString(), (consumeTime - bornTimestamp) / 1000);
            }
        }
        return map;
    }

    /**
     * Check the orderliness of the messages
     *
     * @param dequeueMessages A collection of received messages
     * @return Partition order or not
     */
    private static boolean checkOrder(DataCollector<Object> dequeueMessages) {
        Collection<Object> receivedMessages = dequeueMessages.getAllData();
        ConcurrentHashMap<String, LinkedList<Object>> map = new ConcurrentHashMap<>();
        for (Object receivedMessage : receivedMessages) {
            MessageView messageView = (MessageView) receivedMessage;
            String shardingKey = String.valueOf(messageView.getMessageGroup());
            LinkedList<Object> messages;
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
     * Check that all the messages sent are consumed
     *
     * @param enqueueMessages Send the enqueuing message set
     * @param dequeueMessages Consume the outgoing message set
     * @param timeoutMills    Checking the timeout Period
     * @param consumedTimes
     * @return A collection of messages that are not consumed
     */
    private static Collection<Object> waitForMessageConsume(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages,
        Long timeoutMills, Integer consumedTimes) {
        log.info("Set timeout: {}ms", timeoutMills);

        Collection<Object> sendMessages = new ArrayList<>(enqueueMessages.getAllData());

        long currentTime = System.currentTimeMillis();

        while (!sendMessages.isEmpty()) {
//            log.info("param1:{}, param2:{}", enqueueMessages.getDataSize(), dequeueMessages.getDataSize());
            List<Object> receivedMessagesCopy = new ArrayList<>(dequeueMessages.getAllData());
            Iterator<Object> iter = sendMessages.iterator();
            while (iter.hasNext()) {
                String enqueueMessageId = (String) iter.next();
//                String messageId = (MessageView) message;

                long msgCount = receivedMessagesCopy.stream().filter(
                    msg -> {
                        MessageView messageView = (MessageView) msg;
                        return messageView.getMessageId().toString().equals(enqueueMessageId);
                    }).count();
                if (msgCount > 0 && getRepeatedTimes(receivedMessagesCopy, enqueueMessageId) == consumedTimes) {
                    iter.remove();
                } else if (getRepeatedTimes(receivedMessagesCopy, enqueueMessageId) > consumedTimes) {
                    Assertions.fail(
                        String.format("More retry messages were consumed than expected (including one original message) Except:%s, Actual:%s, MsgId:%s", consumedTimes, getRepeatedTimes(receivedMessagesCopy, enqueueMessageId),
                            enqueueMessageId));
                    //log.error("More retry messages were consumed than expected, Except:{}, Actual:{}", consumedTimes, getRepeatedTimes(receivedMessagesCopy, message));
                }
            }
            if (sendMessages.isEmpty()) {
                break;
            }

            if (System.currentTimeMillis() - currentTime >= timeoutMills) {
                log.error("Timeout but not received all send messages,topic:{},  send {} , recv {}, not received msg: {}\n received msg:{}\n",
                    dequeueMessages.getDataSize() > 0 ? ((MessageView) dequeueMessages.getFirstElement()).getTopic() : null, enqueueMessages.getDataSize(), receivedMessagesCopy.size(), sendMessages, receivedMessagesCopy);
                break;
            }
            TestUtils.waitForMoment(500L);
        }
        return sendMessages;
    }

    private static synchronized int getRepeatedTimes(Collection<Object> recvMsgs, String enqueueMessageId) {
        int count = 0;
        for (Object recvMsg : recvMsgs) {
            MessageView messageView = (MessageView) recvMsg;
            if (messageView.getMessageId().toString().equals(enqueueMessageId)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Validation of sql attribute filtering
     *
     * @param enqueueMessages A message sent
     * @param dequeueMessages News of consumption
     * @param props           The desired attribute condition is not met
     */
    public static void verifyNormalMessageWithUserProperties(DataCollector<Object> enqueueMessages,
        DataCollector<Object> dequeueMessages, HashMap<String, String> props, int expectedUnrecvMsgNum) {
        Collection<Object> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT * 1000L, 1);
        Collection<Object> recvMsgs = dequeueMessages.getAllData();
        for (Object unConsumedMessage : recvMsgs) {
            MessageView messageView = (MessageView) unConsumedMessage;
            for (Map.Entry<String, String> entry : props.entrySet()) {
                Map<String, String> msgProperties = messageView.getProperties();
                for (Map.Entry<String, String> property : msgProperties.entrySet()) {
                    if (property.getKey().equals(entry.getKey()) && property.getValue().equals(entry.getValue())) {
                        Assertions.fail("sql attribute filtering is not in effect, consuming messages to other attributes," + messageView.getProperties().toString());
                    }
                }
            }
        }
        if (unConsumedMessages.size() != expectedUnrecvMsgNum) {
            Assertions.fail("Failed to consume all the sent data by sql filter");
        }
    }

    /**
     * Verifying Cluster Consumption
     *
     * @param enqueueMessages    All messages sent
     * @param dequeueAllMessages Multiple consumer end, consumption of all messages
     */
    @SafeVarargs
    public static void verifyClusterConsume(DataCollector<Object> enqueueMessages,
        DataCollector<Object>... dequeueAllMessages) {
        long currentTime = System.currentTimeMillis();
        List<Object> sendMessagesCopy = new ArrayList<>(enqueueMessages.getAllData());

        while (!sendMessagesCopy.isEmpty()) {
            Collection<Object> noDupMsgs = new ArrayList<>();
            for (DataCollector<Object> messages : dequeueAllMessages) {
                noDupMsgs.addAll(messages.getAllData());
                //log.info("consumer received message: {}", messages.getDataSize());
            }
            log.info("sendMessagesCopy left: {}", sendMessagesCopy.size());

            List<Object> receivedMessagesCopy = new ArrayList<>(noDupMsgs);
            Iterator<Object> iter = sendMessagesCopy.iterator();
            while (iter.hasNext()) {
                Object object = iter.next();
                String messageId = (String) object;
                long msgCount = receivedMessagesCopy.stream().filter(msg -> ((MessageView) msg).getMessageId().toString().equals(messageId)).count();
                if (msgCount > 0 && getRepeatedTimes(receivedMessagesCopy, messageId) == 1) {
                    iter.remove();
                }
            }
            if (sendMessagesCopy.isEmpty()) {
                break;
            }
            if (System.currentTimeMillis() - currentTime >= 60000L) {
                log.error("Timeout but not received all send messages, not received msg: {}\n received msg:{}\n", sendMessagesCopy, receivedMessagesCopy);
                break;
            }
            TestUtils.waitForMoment(500L);
        }

        Assertions.assertEquals(0, sendMessagesCopy.size(), String.format("The following %s messages are not consumed: %s", sendMessagesCopy.size(), sendMessagesCopy));

    }

    /**
     * Verify no consumption for a period of time
     *
     * @param receivedMessages
     * @param timeout          time
     */
    public static void waitForConsumeFailed(DataCollector<Object> receivedMessages, int timeout) {
        long currentTime = System.currentTimeMillis();
        while (currentTime + timeout * 1000L > System.currentTimeMillis()) {
            if (receivedMessages.getDataSize() > 0) {
                Assertions.fail(String.format("Consumed %s messages", receivedMessages.getDataSize()));
                break;
            }
            TestUtils.waitForSeconds(1);
        }
    }

    public static void verifyNormalMessage(RMQNormalProducer producer, RMQNormalConsumer consumer) {
        log.info("The verification process starts......");
        Collection<Object> unConsumedMessages = waitForMessageConsume(producer.getEnqueueMessages(), consumer.getDequeueMessages(), TIMEOUT * 1000L, 1);
        if (unConsumedMessages.size() > 0) {
            Assertions.fail(String.format("The following %s messages are not consumed: %s", unConsumedMessages.size(), unConsumedMessages));
        }
    }

    public static void tryReceiveOnce(SimpleConsumer consumer) {
        tryReceiveOnce(consumer, false, false);
    }

    public static void tryReceiveOnce(SimpleConsumer consumer, Boolean useExistTopic, Boolean useExistGid) {
        long start = System.currentTimeMillis();
        if (!useExistTopic || !useExistGid) {
            CompletableFuture[] cfs = new CompletableFuture[5];
            for (int i = 0; i < 5; i++) {
                log.info("Try pulling a message once");
                CompletableFuture<List<MessageView>> future = consumer.receiveAsync(32, Duration.ofSeconds(10));
                future.thenAccept(messageViews -> {
                    if (messageViews.size() > 0) {
                        for (MessageView messageView : messageViews) {
                            receivedIndex.getAndIncrement();
                            log.info("MessageId:{}, Body:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                            try {
                                consumer.ack(messageView);
                            } catch (ClientException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                });
                cfs[i] = future;
            }
            try {
                CompletableFuture.allOf(cfs).get(30, TimeUnit.SECONDS);
                log.info("receive server response, cost={}ms", System.currentTimeMillis() - start);
            } catch (Exception e) {
                throw new RuntimeException("receive response count not match");
            }
        }
    }

    public static void waitReceiveThenAckAsync(RMQNormalProducer producer, SimpleConsumer consumer, int maxMessageNum,
        Duration invisibleDuration) {
        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        List<Runnable> runnables = new ArrayList<>();
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());
        for (int i = 0; i < defaultSimpleThreadNums; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (endTime > System.currentTimeMillis()) {
                            final List<MessageView> messageViews = consumer.receive(maxMessageNum, invisibleDuration);
                            if (messageViews.size() > 0) {
                                for (MessageView messageView : messageViews) {
                                    receivedIndex.getAndIncrement();
                                    log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                        StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                                    CompletableFuture<Void> future = consumer.ackAsync(messageView);
                                    future.thenAccept(new Consumer<Void>() {
                                        @Override
                                        public void accept(Void unused) {
                                            log.info("ack async success");
                                            sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                                        }
                                    });
                                    future.exceptionally(new Function<Throwable, Void>() {
                                        @Override
                                        public Void apply(Throwable throwable) {
                                            throwable.printStackTrace();
                                            Assertions.fail(String.format("ack async failed: %s", throwable.getMessage()));
                                            return null;
                                        }
                                    });
                                    try {
                                        future.get();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                            if (sendCollection.size() == 0) {
                                break;
                            }
                        }
                        Assertions.assertEquals(0, sendCollection.size(), String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
                    } catch (ClientException e) {
                        Assertions.fail(e.getMessage());
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, TIMEOUT + 5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitReceiveThenAck(RMQNormalProducer producer, SimpleConsumer consumer, int maxMessageNum,
        Duration invisibleDuration) {
        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;

        List<Runnable> runnables = new ArrayList<>(8);
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());
        for (int i = 0; i < defaultSimpleThreadNums; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (endTime > System.currentTimeMillis()) {
                            final List<MessageView> messageViews = consumer.receive(maxMessageNum, invisibleDuration);
                            if (messageViews.size() > 0) {
                                for (MessageView messageView : messageViews) {
                                    receivedIndex.getAndIncrement();
                                    log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                        StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                                    consumer.ack(messageView);
                                    sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                                }
                            }
                            log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                            if (sendCollection.size() == 0) {
                                break;
                            }
                        }
                        Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
                    } catch (ClientException e) {
                        Assertions.fail(e.getMessage());
                    }
                }
            });
        }

        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, TIMEOUT + 5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitFIFOReceiveThenAck(RMQNormalProducer producer, SimpleConsumer consumer,
        int maxMessageNum,
        Duration invisibleDuration) {
        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<Object> sendCollection = producer.getEnqueueMessages().getAllData();
        ConcurrentHashMap<String, LinkedList<Object>> map = new ConcurrentHashMap<>();
        try {
            while (endTime > System.currentTimeMillis()) {
                final List<MessageView> messageViews = consumer.receive(maxMessageNum, invisibleDuration);
                if (messageViews.size() > 0) {
                    for (MessageView messageView : messageViews) {
                        receivedIndex.getAndIncrement();
                        log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                            StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                        consumer.ack(messageView);
                        sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                        String shardingKey = String.valueOf(messageView.getMessageGroup());
                        LinkedList<Object> messages;
                        if (map.containsKey(shardingKey)) {
                            messages = map.get(shardingKey);
                            messages.add(messageView);
                        } else {
                            messages = new LinkedList<>();
                            messages.add(messageView);
                            map.put(shardingKey, messages);
                        }
                    }
                }
                log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                if (sendCollection.size() == 0) {
                    Assertions.assertTrue(checkOrderMessage(map), "Consumption is not sequential");
                }
            }
            Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
        } catch (ClientException e) {
            Assertions.fail(e.getMessage());
        }
    }

    public static void waitReceiveAsyncThenAck(RMQNormalProducer producer, SimpleConsumer consumer,
        int maxMessageNum,
        Duration invisibleDuration) {
        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        while (endTime > System.currentTimeMillis()) {
            if (sendCollection.size() == 0) {
                break;
            }
            final CompletableFuture<List<MessageView>> future = consumer.receiveAsync(maxMessageNum, invisibleDuration);
            future.thenAcceptAsync(new Consumer<List<MessageView>>() {
                @Override
                public void accept(List<MessageView> messageViews) {
                    log.info("Get {} message: {}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                    if (messageViews.size() > 0) {
                        for (MessageView messageView : messageViews) {
                            receivedIndex.getAndIncrement();
                            log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                            try {
                                consumer.ack(messageView);
                                sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                            } catch (ClientException e) {
                                e.printStackTrace();
                            }
                        }
                        log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                    }
                }
            }, executorService);
            future.exceptionally(new Function<Throwable, List<MessageView>>() {
                @Override
                public List<MessageView> apply(Throwable throwable) {
                    throwable.printStackTrace();
//                    log.info("received async failed:{}", throwable.getMessage());
                    Assertions.fail(String.format("received async failed:%s", throwable.getMessage()));
                    return null;
                }
            });
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Assertions.assertEquals(0, sendCollection.size(), String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
    }

    public static void waitReceiveAsyncThenAckAsync(RMQNormalProducer producer, SimpleConsumer consumer,
        int maxMessageNum, Duration invisibleDuration) {
        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        while (endTime > System.currentTimeMillis()) {
            if (sendCollection.size() == 0) {
                break;
            }
            final CompletableFuture<List<MessageView>> future = consumer.receiveAsync(maxMessageNum, invisibleDuration);
            future.thenAcceptAsync(new Consumer<List<MessageView>>() {
                @Override
                public void accept(List<MessageView> messageViews) {
                    log.info("Get {} message: {}", messageViews.size(), Arrays.toString(messageViews.toArray()));
                    if (messageViews.size() > 0) {
                        for (MessageView messageView : messageViews) {
                            receivedIndex.getAndIncrement();
                            log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                            CompletableFuture<Void> ackFuture = consumer.ackAsync(messageView);
                            ackFuture.thenAccept(new Consumer<Void>() {
                                @Override
                                public void accept(Void unused) {
                                    log.info("ack async success");
                                    sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                                }
                            });
                            ackFuture.exceptionally(new Function<Throwable, Void>() {
                                @Override
                                public Void apply(Throwable throwable) {
                                    throwable.printStackTrace();
                                    Assertions.fail(String.format("ack async failed: %s", throwable.getMessage()));
                                    return null;
                                }
                            });
                            try {
                                ackFuture.get();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                    }
                }
            }, executorService);
            future.exceptionally(new Function<Throwable, List<MessageView>>() {
                @Override
                public List<MessageView> apply(Throwable throwable) {
                    throwable.printStackTrace();
                    Assertions.fail(String.format("received async failed:%s", throwable.getMessage()));
                    return null;
                }
            });
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));

    }

    public static void waitReceiveThenNack(RMQNormalProducer producer, SimpleConsumer consumer, int maxMessageNum,
        Duration receiveInvisibleDuration, Duration changeInvisibleDuration) {

        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<Object> sendCollection = producer.getEnqueueMessages().getAllData();
        try {
            while (endTime > System.currentTimeMillis()) {
                final List<MessageView> messageViews = consumer.receive(maxMessageNum, receiveInvisibleDuration);
                if (messageViews.size() > 0) {
                    for (MessageView messageView : messageViews) {
                        receivedIndex.getAndIncrement();
                        log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                            StandardCharsets.UTF_8.decode(messageView.getBody()).toString(), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                        if (changeInvisibleDuration != null) {
                            consumer.changeInvisibleDuration(messageView, changeInvisibleDuration);
                            log.info("Change the invisibility duration of [{}] to (changeInvisibleDuration): {}", messageView.getMessageId().toString(), changeInvisibleDuration);
                        }
                        sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                    }
                }
                log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                if (sendCollection.size() == 0) {
                    break;
                }
            }
            Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
        } catch (ClientException e) {
            Assertions.fail(e.getMessage());
        }
    }

    public static void waitReceiveThenAck(RMQNormalProducer producer, SimpleConsumer consumer, int maxMessageNum,
        Duration invisibleDuration, int consumeTimes) {
        ConcurrentHashMap<String, Integer> msgMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> msgTimeMap = new ConcurrentHashMap<>();
        List<Runnable> runnables = new ArrayList<>(4);
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());

        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
                    try {
                        while (endTime > System.currentTimeMillis()) {
                            final List<MessageView> messageViews = consumer.receive(maxMessageNum, invisibleDuration);
                            if (messageViews.size() > 0) {
                                for (MessageView messageView : messageViews) {
                                    receivedIndex.getAndIncrement();
                                    log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                        StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                                    String receivedMessageId = messageView.getMessageId().toString();

                                    //Processing repeated consuming messages
                                    if (msgMap.containsKey(receivedMessageId)) {
                                        msgMap.put(receivedMessageId, msgMap.get(receivedMessageId) + 1);
                                    } else {
                                        msgMap.put(receivedMessageId, 1);
                                    }

                                    //Calculate the time between consumption
                                    if (msgTimeMap.containsKey(receivedMessageId)) {
                                        Long preTime = msgTimeMap.get(receivedMessageId);
                                        Long currentTime = System.currentTimeMillis();
                                        msgTimeMap.put(receivedMessageId, currentTime);
                                        //Determine the time error of the retry message
                                        if ((currentTime - preTime) < (invisibleDuration.getSeconds() - 3) * 1000 || (currentTime - preTime) > (invisibleDuration.getSeconds() + 3) * 1000) {
                                            String info = String.format("currentTime:%s, preTime:%s, invisibleDuration:%s, actual:%s", DateUtils.format(currentTime), DateUtils.format(preTime), invisibleDuration.getSeconds() * 1000, currentTime - preTime);
                                            log.info(info);
                                            Assertions.fail(info + String.format(", Not between %s and %s", invisibleDuration.getSeconds() - 2, invisibleDuration.getSeconds() + 2));
                                        }
                                    } else {
                                        msgTimeMap.put(receivedMessageId, System.currentTimeMillis());
                                    }

                                    if (msgMap.get(receivedMessageId).equals(consumeTimes)) {
                                        consumer.ack(messageView);
                                    }
                                }
                            }
                            sendCollection.removeIf(sendMessageId -> getRepeatedTimes(msgMap, sendMessageId) == consumeTimes);

                            log.info("Pull message: {} bar, remaining unconsumed message: {} bar, need to consume: {} times", messageViews.size(), sendCollection.size(), consumeTimes);
                            if (sendCollection.size() == 0) {
                                break;
                            }
                        }
                        Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
                    } catch (ClientException e) {
                        Assertions.fail(e.getMessage());
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, TIMEOUT + 5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitReceiveThenAck(RMQNormalProducer producer, SimpleConsumer consumer, int maxMessageNum,
        Duration invisibleDuration, int consumeTimes, int waitTime, boolean needAck) {
        ConcurrentHashMap<String, Integer> msgMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> msgTimeMap = new ConcurrentHashMap<>();
        List<Runnable> runnables = new ArrayList<>(4);
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());

        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    long endTime = System.currentTimeMillis() + waitTime * 1000;
                    try {
                        while (endTime > System.currentTimeMillis()) {
                            final List<MessageView> messageViews = consumer.receive(maxMessageNum, invisibleDuration);
                            if (messageViews.size() > 0) {
                                for (MessageView messageView : messageViews) {
                                    receivedIndex.getAndIncrement();
                                    log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                        StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                                    String receivedMessageId = messageView.getMessageId().toString();

                                    //Processing repeated consuming messages
                                    if (msgMap.containsKey(receivedMessageId)) {
                                        msgMap.put(receivedMessageId, msgMap.get(receivedMessageId) + 1);
                                    } else {
                                        msgMap.put(receivedMessageId, 1);
                                    }

                                    //Calculate the time between consumption
                                    if (msgTimeMap.containsKey(receivedMessageId)) {
                                        Long preTime = msgTimeMap.get(receivedMessageId);
                                        Long currentTime = System.currentTimeMillis();
                                        msgTimeMap.put(receivedMessageId, currentTime);
                                        //Determine the time error of the retry message
                                        if ((currentTime - preTime) < (invisibleDuration.getSeconds() - 2) * 1000 || (currentTime - preTime) > (invisibleDuration.getSeconds() + 2) * 1000) {
                                            String info = String.format("currentTime:%s, preTime:%s, invisibleDuration:%s, actual:%s", currentTime, preTime, invisibleDuration.getSeconds() * 1000, currentTime - preTime);
                                            log.info(info);
//                                            Assertions.fail(info + String.format(", Not between %s and %s", invisibleDuration.getSeconds() - 2, invisibleDuration.getSeconds() + 2));
                                        }
                                    } else {
                                        msgTimeMap.put(receivedMessageId, System.currentTimeMillis());
                                    }

                                    if (needAck) {
                                        if (msgMap.get(receivedMessageId).equals(consumeTimes)) {
                                            consumer.ack(messageView);
                                        }
                                    }
                                }
                            }
                            if (needAck) {
                                sendCollection.removeIf(sendMessageId -> getRepeatedTimes(msgMap, sendMessageId) == consumeTimes);
                            }
                            log.info("Pull message: {} bar, remaining unconsumed message: {} bar, need to consume: {} times", messageViews.size(), sendCollection.size(), consumeTimes);

                            if (sendCollection.size() == 0) {
                                break;
                            }
                        }

                        if (needAck) {
                            Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
                        } else {
                            for (Object obj : sendCollection) {
                                if (msgMap.containsKey((String) obj)) {
                                    log.info("Retry times: {}", msgMap.get((String) obj));
                                    if (msgMap.get((String) obj) != consumeTimes) {
                                        Assertions.fail(String.format("[ %s ] the number of retry times is invalid, expect:%s, actual:%s", obj, msgMap.get((String) obj), consumeTimes));
                                    }
                                }
                            }
                        }
                    } catch (ClientException e) {
                        Assertions.fail(e.getMessage());
                    }
                }
            });
        }
        try {
            AssertUtils.assertConcurrent("Test Failed", runnables, TIMEOUT + 5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitDelayReceiveThenAck(RMQNormalProducer producer, SimpleConsumer consumer,
        int maxMessageNum, long delayTimeMillis) {
        long endTime = System.currentTimeMillis() + TIMEOUT * 1000;
        Collection<Object> sendCollection = Collections.synchronizedCollection(producer.getEnqueueMessages().getAllData());
        List<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (endTime > System.currentTimeMillis()) {
                            final List<MessageView> messageViews = consumer.receive(maxMessageNum, Duration.ofSeconds(10));
                            if (messageViews.size() > 0) {
                                for (MessageView messageView : messageViews) {
                                    receivedIndex.getAndIncrement();
                                    log.info("MessageId:{}, Body:{}, tag:{}, Property:{}, Index:{}, Retry:{}", messageView.getMessageId(),
                                        StandardCharsets.UTF_8.decode(messageView.getBody()), messageView.getTag().get(), messageView.getProperties(), receivedIndex.get(), messageView.getDeliveryAttempt());
                                    consumer.ack(messageView);
                                    long bornTimestamp = messageView.getBornTimestamp();
                                    long startDeliverTime = messageView.getDeliveryTimestamp().get();
                                    //Check the current time and the distribution time. If the difference is within 5s, the requirements are met
                                    if (Math.abs((startDeliverTime - bornTimestamp) - delayTimeMillis) > 5 * 1000L) {
                                        Assertions.fail(String.format("Message [%s] does not meet latency requirements, TIMER_ENQUEUE_MS:%s, startDeliverTime:%s, delay:%s", messageView.getMessageId().toString(), bornTimestamp, startDeliverTime, startDeliverTime - bornTimestamp));
                                    }
                                    sendCollection.removeIf(sendMessageId -> sendMessageId.equals(messageView.getMessageId().toString()));
                                }
                            }
                            log.info("Pull message: {} bar, remaining unconsumed message: {} bar", messageViews.size(), sendCollection.size());
                            if (sendCollection.size() == 0) {
                                break;
                            }
                        }
                        Assertions.assertTrue(sendCollection.size() == 0, String.format("Remaining [%s] unconsumed messages: %s", sendCollection.size(), Arrays.toString(sendCollection.toArray())));
                    } catch (ClientException e) {
                        e.printStackTrace();
                        Assertions.fail(e.getMessage());
                    }
                }
            });
        }

        try {
            AssertUtils.assertConcurrent("Test Failed ", runnables, TIMEOUT + 15);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static int getRepeatedTimes(ConcurrentHashMap<String, Integer> receivedMsg, Object sendMessageId) {
        if (receivedMsg.containsKey((String) sendMessageId)) {
            return receivedMsg.get((String) sendMessageId);
        }
        return 0;
    }
}
