package org.apache.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class RocketMQApplication {
    protected static List<String> nameserverIpList = new ArrayList<>();
    protected static String nameserverPort = "9876";

    public static void main(String[] args) {
        SpringApplication.run(RocketMQApplication.class, args);
    }
}
