package org.apache.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.utils.MQAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.LinkedList;
import org.springframework.core.env.Environment;
@Slf4j
@Component
@RocketMQMessageListener(topic = "${rocketmq.test.topic.delay}", consumerGroup = "gid-delay-spring-test", selectorExpression = "*")
public class DelayListenerImpl implements RocketMQListener<MessageExt> {

    //  <MessageId, ReceivedTime>
    private HashMap<String, Long> receivedMessages = new HashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    @Override
    public void onMessage(MessageExt msg) {
        log.info("Received message: " + msg);
        lastMessage = msg.getMsgId();
        receivedMessages.put(msg.getMsgId(), System.currentTimeMillis());
        receivedMessageIds.add(msg.getMsgId());
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public HashMap<String, Long> getReceivedMessages() {
        return receivedMessages;
    }

    public LinkedList<String> getReceivedMessageIds() {
        return receivedMessageIds;
    }
}
