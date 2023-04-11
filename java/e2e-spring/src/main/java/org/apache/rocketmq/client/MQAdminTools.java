package org.apache.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.utils.MQAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class MQAdminTools {
    protected static List<String> nameserverIpList = new ArrayList<>();
    protected static String nameserverPort = "9876";
    @Value("${rocketmq.name-server}")
    private String nameserver;
    @Value("${rocketmq.cluster}")
    private String cluster;
    @Value("${rocketmq.test.topic.normal}")
    private String normalTopic;
    @Value("${rocketmq.test.topic.delay}")
    private String delayTopic;
    @Value("${rocketmq.test.topic.order}")
    private String orderTopic;
    @Value("${rocketmq.test.topic.trans}")
    private String transTopic;

    @PostConstruct
    private void init() {
        MQAdmin.initMQAdminExtInstance(nameserver);
        boolean result1 = MQAdmin.createTopic(cluster, normalTopic, 8, TopicMessageType.NORMAL.getValue());
        boolean result2 = MQAdmin.createTopic(cluster, delayTopic, 8, TopicMessageType.NORMAL.getValue());
        boolean result3 = MQAdmin.createTopic(cluster, orderTopic, 8, TopicMessageType.NORMAL.getValue());
        boolean result4 = MQAdmin.createTopic(cluster, transTopic, 8, TopicMessageType.NORMAL.getValue());
        log.info("Create topic[{}] {}", normalTopic, result1);
        log.info("Create topic[{}] {}", normalTopic, result2);
        log.info("Create topic[{}] {}", normalTopic, result3);
        log.info("Create topic[{}] {}", normalTopic, result4);
    }

//    private static void initConnectionInfo() {
//        String ALL_IP = System.getProperty("ALL_IP");
//        if (ALL_IP != null) {
//            String[] allPodInfos = ALL_IP.split(",");
//            for (String podInfo : allPodInfos) {
//                if (podInfo.contains("nameserver")) {
//                    nameserverIpList.add(podInfo.substring(podInfo.indexOf(":") + 1));
//                }
//            }
//            if (nameserverIpList.isEmpty()) {
//                log.warn("INIT- Get nameserver from external is empty");
//            } else {
//                String namesrvAddr = nameserverIpList.get(0) + ":" + nameserverPort;
//                System.setProperty("rocketmq.name-server", namesrvAddr);
//            }
//        }
//    }

}
