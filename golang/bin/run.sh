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


# cd project base dir to compile mqadmin utils for other language e2e test using
cd ../common &&  mvn -Prelease -DskipTests clean package -U
# set env for mqadmin (use source to set linux env variables in current shell)
cd ../rocketmq-admintools && source bin/env.sh
# run go e2e test case with latest client version
cd ../golang && go get -u github.com/apache/rocketmq-clients/golang && go test ./mqgotest/... -timeout 2m  -v && go test ./pkg/... -timeout 2m  -v
