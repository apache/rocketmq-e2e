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

using Utils;

namespace Rocketmq.Tests
{
    public class BaseTest
    {
        protected const int sendNum = 10;
        protected string accessKey = string.IsNullOrEmpty(ClientUtils.ACCESS_KEY)? "":ClientUtils.ACCESS_KEY;
        protected string secretKey = string.IsNullOrEmpty(ClientUtils.SECRET_KEY)? "":ClientUtils.SECRET_KEY;
        protected string nameserver = ClientUtils.NAMESERVER;
        protected string endpoints = ClientUtils.GRPC_ENDPOINT;
        protected string clusterName = ClientUtils.CLUSTER_NAME;
        protected string brokerAddr = ClientUtils.BROKER_ADDR;
    }
}