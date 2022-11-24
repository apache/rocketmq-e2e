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
import java.util.Properties;
import org.apache.commons.codec.digest.DigestUtils;
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
    protected static String cluster;
    protected static MQAdmin mqAdmin;

    static {
        initResource();
        MQAdmin.initMQAdminExtInstance(namesrvAddr);
        log.info("init process success");
    }

    //Initializing the console
    private static void initResource() {
        Properties properties = null;
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

        endPoint = System.getProperty("endPoint", properties.getProperty("endPoint"));
        namesrvAddr = System.getProperty("namesrvAddr", properties.getProperty("namesrvAddr"));
        cluster = System.getProperty("cluster", properties.getProperty("cluster"));
        aclEnable = Boolean.parseBoolean(System.getProperty("aclEnable", properties.getProperty("aclEnable", "false")));

        if (aclEnable) {
            String instanceUsername = System.getProperty("INSTANCE_USERNAME", properties.getProperty("INSTANCE_USERNAME"));
            String instancePassword = System.getProperty("INSTANCE_PASSWORD", properties.getProperty("INSTANCE_PASSWORD"));
            account = new Account(instanceUsername, instancePassword);
            log.info("INIT - ENV [instanceUsername:{}, instancePassword:{}]", instanceUsername, instancePassword);
        } else {
            account = new Account(endPoint);
        }
        String testDomain = endPoint.substring(0, endPoint.indexOf(":"));
        String testPort = endPoint.substring(endPoint.lastIndexOf(":") + 1);
        //tcp endpoint connect check
        boolean isEndPointConnected = telnet(testDomain, Integer.parseInt(testPort), 5000);
        if (!isEndPointConnected) {
            log.error("INIT - TCP endpoint connectivity check: telnet {} {} ==> timeout: {}, isConnected: false", testDomain, testPort, 5000);
            log.error("INIT - Warning, exit!!!");
            System.exit(0);
        } else {
            log.info("INIT - TCP endpoint connectivity check: telnet {} {} ==> isConnected: true", testDomain, testPort);
        }

        log.info("INIT - ENV [endpoint:{}]", endPoint);
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

    /**
     * Generated md5
     *
     * @param className  className
     * @param methodName methodName
     * @return md5
     */
    protected static String getMD5Sum(String className, String methodName) {
        String completeName = String.format("%s_%s", className, methodName);
        return methodName + "_" + DigestUtils.md5Hex(completeName).substring(0, 6);
    }

    public static boolean telnet(String hostname, int port, int timeout) {
        Socket socket = new Socket();
        boolean isConnected = false;
        try {
            socket.connect(new InetSocketAddress(hostname, port), timeout); // connection
            isConnected = socket.isConnected(); // Check the connectivity status using existing methods
        } catch (IOException e) {
            log.error("telnet test false");        // When the connection is disconnected, throw the exception directly, the exception can be captured
        } finally {
            try {
                socket.close();   // Closing the connection
            } catch (IOException e) {
                log.error("telnet test false");
            }
        }
        return isConnected;
    }
}
