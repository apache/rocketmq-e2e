#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

nameserverkey="nameserver"
brokerkey="broker"
proxykey="proxy"

if [ -z "$ALL_IP" ] ; then
    echo "ERROR: ENV ALL_IP  not exists!!";
    exit 1;
  else
     allip=(`echo $ALL_IP | tr ',' ' '`)
     for i in "${allip[@]}"
     do
     if  result=$(echo $i | grep "${nameserverkey}");then
          echo "NAMESERVER=${i#*:}:9876";
          export NAMESERVER=${i#*:}:9876 ;
     elif result=$(echo $i | grep "${proxykey}"); then
     #       opensource 8081
          echo "GRPC_ENDPOINT=${i#*:}:8081";
          export GRPC_ENDPOINT=${i#*:}:8081 ;
     elif result=$(echo $i | grep "${brokerkey}"); then
          echo "BROKER_ADDR=${i#*:}:10911";
          export BROKER_ADDR=${i#*:}:10911 ;
     fi
     done
fi

if [ -z "$CLUSTER_NAME" ] ; then
    export CLUSTER_NAME=DefaultCluster ;
fi

