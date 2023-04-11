package org.apache.rocketmq.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.SpringBootBaseTest;
import org.apache.rocketmq.client.NormalListenerImpl;
import org.apache.rocketmq.client.TransactionListenerImpl;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class TransMessageTest extends SpringBootBaseTest {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    @Autowired
    private NormalListenerImpl consumer;
    @Autowired
    private TransactionListenerImpl transactionListener;
    @Value("${rocketmq.test.topic.normal}")
    private String normalTopic;

    @Test
    public void testTransExecuteCommitMessage() {
        String message = "Hello, RocketMQ!";
        transactionListener.setExecuteState(RocketMQLocalTransactionState.COMMIT);
        SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(normalTopic, MessageBuilder.withPayload(message).build(), null);
        log.info(sendResult.getMsgId());
        Assertions.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        Assertions.assertNotNull(sendResult.getTransactionId());
        await().atMost(10, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return consumer.getReceivedMessageIds().contains(sendResult.getMsgId());
            }
        });
    }

    @Test
    public void testTransExecuteRollbackMessage() {
        String message = "Hello, RocketMQ!";
        transactionListener.setExecuteState(RocketMQLocalTransactionState.ROLLBACK);
        SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(normalTopic, MessageBuilder.withPayload(message).build(), null);
        log.info(sendResult.getMsgId());
        Assertions.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        Assertions.assertNotNull(sendResult.getTransactionId());

        assertThrows(ConditionTimeoutException.class, () -> {
            await().atLeast(10, SECONDS).until(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    return consumer.getReceivedMessageIds().contains(sendResult.getMsgId());
                }
            });
        }, "Expected ConditionTimeoutException to throw, but it didn't");
    }

    @Test
    public void testTransExecuteUnknowAndCheckCOMMITMessage() {
        String message = "Hello, RocketMQ!";
        transactionListener.setExecuteState(RocketMQLocalTransactionState.UNKNOWN);
        transactionListener.setCheckState(RocketMQLocalTransactionState.COMMIT);
        SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(normalTopic, MessageBuilder.withPayload(message).build(), null);
        log.info(sendResult.getMsgId());
        Assertions.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        Assertions.assertNotNull(sendResult.getTransactionId());
        await().atMost(30, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return consumer.getReceivedMessageIds().contains(sendResult.getMsgId());
            }
        });
    }

    @Test
    public void testTransExecuteUnknowAndCheckRollbackMessage() {
        String message = "Hello, RocketMQ!";
        transactionListener.setExecuteState(RocketMQLocalTransactionState.UNKNOWN);
        transactionListener.setCheckState(RocketMQLocalTransactionState.ROLLBACK);
        SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(normalTopic, MessageBuilder.withPayload(message).build(), null);
        log.info(sendResult.getMsgId());
        Assertions.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        Assertions.assertNotNull(sendResult.getTransactionId());

        assertThrows(ConditionTimeoutException.class, () -> {
            await().atLeast(10, SECONDS).until(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    return consumer.getReceivedMessageIds().contains(sendResult.getMsgId());
                }
            });
        }, "Expected ConditionTimeoutException to throw, but it didn't");
    }
}
