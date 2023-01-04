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

package org.apache.rocketmq.account;

public class Account {
    /**
     * aliyun AccessKey
     */
    protected String accessKey;
    /**
     * aliyun SecretKey
     */
    protected String secretKey;
    /**
     * instanceUserName
     */
    protected String instanceUserName;
    /**
     * instancePassword
     */
    protected String instancePassword;
    /**
     * endpoint
     */
    protected String endpoint;
    /**
     * instanceId
     */
    protected String instanceId;
    /**
     * consoleEndpoint
     */
    protected String consoleEndpoint;
    /**
     * region
     */
    protected String regionId;
    /**
     * Account User ID
     */
    protected String userId;
    /**
     * Account User Name
     */
    protected String accountName;

    public Account() {
    }

    public Account(String endpoint) {
        this.endpoint = endpoint;
    }

    public Account(String instanceUserName, String instancePassword) {
        this.instanceUserName = instanceUserName;
        this.instancePassword = instancePassword;
    }

    public Account(String accessKey, String secretKey, String endpoint) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
    }

    public Account(String accessKey, String secretKey, String endpoint, String instanceId, String consoleEndpoint,
        String regionId, String userId) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.instanceId = instanceId;
        this.consoleEndpoint = consoleEndpoint;
        this.regionId = regionId;
        this.userId = userId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getInstanceUserName() {
        return instanceUserName;
    }

    public void setInstanceUserName(String instanceUserName) {
        this.instanceUserName = instanceUserName;
    }

    public String getInstancePassword() {
        return instancePassword;
    }

    public void setInstancePassword(String instancePassword) {
        this.instancePassword = instancePassword;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getConsoleEndpoint() {
        return consoleEndpoint;
    }

    public void setConsoleEndpoint(String consoleEndpoint) {
        this.consoleEndpoint = consoleEndpoint;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }
}
