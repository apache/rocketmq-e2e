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

using System;

namespace Utils
{
    public class ClientUtils
    {

        public static readonly string NAMESERVER;
        public static readonly string ACCESS_KEY;
        public static readonly string SECRET_KEY;

        public static readonly string GRPC_ENDPOINT;

        public static readonly string BROKER_ADDR;

        public static readonly string CLUSTER_NAME;

        static ClientUtils()
        {
            NAMESERVER = Environment.GetEnvironmentVariable("NAMESERVER");
            GRPC_ENDPOINT = Environment.GetEnvironmentVariable("GRPC_ENDPOINT");
            CLUSTER_NAME = Environment.GetEnvironmentVariable("CLUSTER_NAME");
            BROKER_ADDR = Environment.GetEnvironmentVariable("BROKER_ADDR");
            ACCESS_KEY = Environment.GetEnvironmentVariable("ACCESS_KEY");
            SECRET_KEY = Environment.GetEnvironmentVariable("SECRET_KEY");
        }

    }
}