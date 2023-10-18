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

package org.apache.rocketmq.frame;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.account.Account;
import org.apache.rocketmq.util.MQAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceInit {
    private static final Logger log = LoggerFactory.getLogger(ResourceInit.class);

    /**
     * Read the configuration file to use
     */
    public static SystemConfigFileHelper configFileHelper = new SystemConfigFileHelper();
    public static Account account = null;
    /**
     * Access point Information
     */
    protected static String endPoint = null;
    protected static String namesrvAddr = null;
    protected static Boolean aclEnable = null;
    protected static String allIp;
    protected static String cluster;
    protected static List<String> nameserverIpList = new ArrayList<>();
    protected static List<String> brokerIpList = new ArrayList<>();
    protected static List<String> proxyIpList = new ArrayList<>();
    protected static String nameserverPort = "9876";
    protected static String endPointPort = "8081";
    private static Properties properties = null;

    static {
        initResource();
        MQAdmin.initMQAdminExtInstance(namesrvAddr);
        log.info("Init process success");
    }

    private static void initResource() {
        try {
            String env = System.getProperty("env", "daily");
            String region = System.getProperty("region", "daily");
            InputStream inputStream = new FileInputStream(String.format("src/test/resources/env/%s/%s.conf", env, region));
            log.info("INIT - use config env:{}, config:{}", env, region);
            properties = configFileHelper.loadConfig(inputStream);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            log.info("INIT - execute error,exit the process");
            System.exit(0);
        }
        // init endpoint and nameserver
        initConnectionInfo();
        initAcl();

        String endPointIp = endPoint.substring(0, endPoint.indexOf(":"));
        String endPointPort = endPoint.substring(endPoint.lastIndexOf(":") + 1);
        checkConnection(endPointIp, endPointPort);

        String namesrvAddrIp = namesrvAddr.substring(0, namesrvAddr.indexOf(":"));
        String namesrvAddrPort = namesrvAddr.substring(namesrvAddr.lastIndexOf(":") + 1);
        checkConnection(namesrvAddrIp, namesrvAddrPort);

    }

    private static void initAcl() {
        aclEnable = Boolean.parseBoolean(System.getProperty("aclEnable", properties.getProperty("aclEnable", "false")));
        if (aclEnable) {
            String accessKey = System.getProperty("accessKey", properties.getProperty("accessKey"));
            String secretKey = System.getProperty("secretKey", properties.getProperty("secretKey"));
            account = new Account(accessKey, secretKey, endPoint);
            log.info("INIT - acl is enabled, [accessKey:{}, secretKey:{}]", accessKey, secretKey);
        } else {
            log.info("INIT - acl is disabled");
            account = new Account(endPoint);
        }
    }

    private static void initConnectionInfo() {
        allIp = System.getenv("ALL_IP");
        if (allIp != null) {
            String[] allPodInfos = allIp.split(",");
            for (String podInfo : allPodInfos) {
                if (podInfo.contains("nameserver")) {
                    nameserverIpList.add(podInfo.substring(podInfo.indexOf(":") + 1));
                } else if (podInfo.contains("broker")) {
                    brokerIpList.add(podInfo.substring(podInfo.indexOf(":") + 1));
                } else if (podInfo.contains("proxy")) {
                    proxyIpList.add(podInfo.substring(podInfo.indexOf(":") + 1));
                }
            }
            if (proxyIpList.isEmpty()) {
                if (brokerIpList.isEmpty()) {
                    log.warn("INIT- Get broker from external is empty");
                    endPoint = System.getProperty("endPoint", properties.getProperty("endPoint"));
                } else {
                    endPoint = brokerIpList.get(0) + ":" + endPointPort;
                }
            } else {
                log.info("INIT- Use enable-proxy model or 4.x model");
                endPoint = proxyIpList.get(0) + ":" + endPointPort;
            }
            if (nameserverIpList.isEmpty()) {
                log.warn("INIT- Get nameserver from external is empty");
                namesrvAddr = System.getProperty("namesrvAddr", properties.getProperty("namesrvAddr"));
            } else {
                namesrvAddr = nameserverIpList.get(0) + ":" + nameserverPort;
            }
        } else {
            log.info("INIT- Get ALL_IP is null, use local info");
            endPoint = System.getProperty("endPoint", properties.getProperty("endPoint"));
            namesrvAddr = System.getProperty("namesrvAddr", properties.getProperty("namesrvAddr"));
        }
        cluster = System.getenv("cluster") != null ? System.getenv("cluster") : properties.getProperty("cluster");
        if (cluster == null) {
            log.error("INIT- cluster is null, system exit");
            System.exit(-1);
        }
        log.info("INIT- endPoint:{}, namesrvAddr:{}, cluster:{}", endPoint, namesrvAddr, cluster);
    }

    private static void checkConnection(String ip, String port) {
        boolean isConnected = telnet(ip, Integer.parseInt(port), 5000);
        if (!isConnected) {
            log.error("INIT - check: telnet {} {} ==> timeout: {}, isConnected: false", ip, port, 5000);
            log.error("INIT - Warning, exit!!!");
            System.exit(0);
        } else {
            log.info("INIT - check: telnet {} {} ==> isConnected: true", ip, port);
        }
    }

    public static class SystemConfigFileHelper {
        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        public Properties loadConfig(InputStream inputStream) throws Exception {
            InputStream in = new BufferedInputStream(inputStream);
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        public void update(Properties properties) throws Exception {
            log.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }

    public static boolean telnet(String hostname, int port, int timeout) {
        Socket socket = new Socket();
        boolean isConnected = false;
        try {
            socket.connect(new InetSocketAddress(hostname, port), timeout);
            isConnected = socket.isConnected();
        } catch (IOException e) {
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
        return isConnected;
    }
}
