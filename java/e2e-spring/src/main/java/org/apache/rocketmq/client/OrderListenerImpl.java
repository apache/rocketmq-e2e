package org.apache.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.test.topic.order}", consumerGroup = "gid-order-spring-test", selectorExpression = "*", consumeMode = ConsumeMode.ORDERLY)
public class OrderListenerImpl implements RocketMQListener<MessageExt> {

    private ConcurrentHashMap<String, LinkedList<MessageExt>> messageGroups = new ConcurrentHashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    @Override
    public void onMessage(MessageExt msg) {
        log.info("Received message- queueId:{}, msgId:{}, body:{}", msg.getQueueId(), msg.getMsgId(), new String(msg.getBody()));
        lastMessage = msg.getMsgId();
        String shardingKey = String.valueOf(msg.getQueueId());
        LinkedList<MessageExt> messages = new LinkedList<>();
        if (messageGroups.containsKey(shardingKey)) {
            messages = messageGroups.get(shardingKey);
            messages.add(msg);
        } else {
            messages.add(msg);
            messageGroups.put(shardingKey, messages);
        }
        receivedMessageIds.add(msg.getMsgId());
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public ConcurrentHashMap<String, LinkedList<MessageExt>> getMessageGroups() {
        return messageGroups;
    }

    public LinkedList<String> getReceivedMessageIds() {
        return receivedMessageIds;
    }
}
