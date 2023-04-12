package org.apache.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;

@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.test.topic.normal}", consumerGroup = "gid-normal-spring-test", selectorExpression = "*")
public class NormalListenerImpl implements RocketMQListener<MessageExt> {

    private HashMap<String, String> receivedMessages = new HashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    @Override
    public void onMessage(MessageExt msg) {
        log.info("Received message: " + msg);
        lastMessage = msg.getMsgId();
        log.info("Received messageId: " + msg.getMsgId());
        receivedMessages.put(msg.getMsgId(), new String(msg.getBody()));
        receivedMessageIds.add(msg.getMsgId());
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public HashMap<String, String> getReceivedMessages() {
        return receivedMessages;
    }

    public LinkedList<String> getReceivedMessageIds() {
        return receivedMessageIds;
    }
}
