package org.apache.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;

@Slf4j
@Component
@RocketMQTransactionListener
public class TransactionListenerImpl implements RocketMQLocalTransactionListener {

    private HashMap<String, String> receivedMessages = new HashMap<>();
    private LinkedList<String> receivedMessageIds = new LinkedList<>();
    private String lastMessage;

    private RocketMQLocalTransactionState executeState = RocketMQLocalTransactionState.COMMIT;
    private RocketMQLocalTransactionState checkState = RocketMQLocalTransactionState.COMMIT;

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object o) {
        log.info(message.toString());
        return executeState;
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        return checkState;
    }

    public void setExecuteState(RocketMQLocalTransactionState executeState) {
        log.info("executor state: {}", executeState);
        this.executeState = executeState;
    }

    public void setCheckState(RocketMQLocalTransactionState checkState) {
        log.info("check state: {}", checkState);
        this.checkState = checkState;
    }
}
