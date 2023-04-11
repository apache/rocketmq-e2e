package org.apache.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.utils.MQAdmin;
import org.apache.rocketmq.utils.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;


@Slf4j
@SpringBootTest(classes = RocketMQApplication.class)
public class SpringBootBaseTest {
    protected static String ALL_IP;
    @Value("${rocketmq.cluster}")
    private String cluster;

    protected String getTopic(String messageType, String methodName) {
        String topic = String.format("topic_%s_%s_%s", messageType, methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[Topic] topic:{}, messageType:{}, methodName:{}", topic, messageType, methodName);
        boolean result = MQAdmin.createTopic(cluster, topic, 8, messageType);
        Assertions.assertTrue(result, String.format("Create topic:%s failed", topic));
        return topic;
    }

    protected String getGroupId(String methodName) {
        String groupId = String.format("GID_%s_%s", methodName, RandomUtils.getStringWithCharacter(6));
        log.info("[ConsumerGroupId] groupId:{}, methodName:{}", groupId, methodName);
        return groupId;
    }

}
