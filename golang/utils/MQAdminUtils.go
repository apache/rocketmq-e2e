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

package utils

import (
	"fmt"
	"log"
	"os/exec"
	"path"
	"runtime"
)

var root string

func init() {
	_, filename, _, ok := runtime.Caller(0)
	// get whole project base path
	root = path.Dir(path.Dir(path.Dir(filename)))
	if !ok {
		log.Fatal("get project root path failed")
	}
}

func CreateTopic(topicName string, brokerAddr string, clusterName string, nameserver string) {
	// use absolute path
	command := "sh " + root + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName
	if nameserver != "" {
		command += " -n " + nameserver
	}
	if brokerAddr != "" {
		command += " -b " + brokerAddr
	}
	if clusterName != "" {
		command += " -c " + clusterName
	}
	fmt.Println(command)
	out, err := exec.Command("/bin/bash", "-c", command).Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}

func CreateDelayTopic(topicName string, brokerAddr string, clusterName string, nameserver string) {
	// use absolute path
	command := "sh " + root + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName
	if nameserver != "" {
		command += " -n " + nameserver
	}
	if brokerAddr != "" {
		command += " -b " + brokerAddr
	}
	if clusterName != "" {
		command += " -c " + clusterName
	}
	command += " -a " + "+message.type=DELAY"
	fmt.Println(command)
	out, err := exec.Command("/bin/bash", "-c", command).Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}

func CreateFIFOTopic(topicName string, brokerAddr string, clusterName string, nameserver string) {
	// use absolute path
	command := "sh " + root + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName
	if nameserver != "" {
		command += " -n " + nameserver
	}
	if brokerAddr != "" {
		command += " -b " + brokerAddr
	}
	if clusterName != "" {
		command += " -c " + clusterName
	}
	command += " -a " + "+message.type=FIFO"
	fmt.Println(command)
	out, err := exec.Command("/bin/bash", "-c", command).Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}

func CreateTransactionTopic(topicName string, brokerAddr string, clusterName string, nameserver string) {
	// use absolute path
	command := "sh " + root + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName
	if nameserver != "" {
		command += " -n " + nameserver
	}
	if brokerAddr != "" {
		command += " -b " + brokerAddr
	}
	if clusterName != "" {
		command += " -c " + clusterName
	}
	command += " -a " + "+message.type=TRANSACTION"
	fmt.Println(command)
	out, err := exec.Command("/bin/bash", "-c", command).Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}

func CreateOrderlyConsumerGroup(consumerGroup string, brokerAddr string, clusterName string, nameserver string) {
	// use absolute path
	command := "sh " + root + "/rocketmq-admintools/bin/mqadmin updateSubGroup -g " + consumerGroup
	if nameserver != "" {
		command += " -n " + nameserver
	}
	if brokerAddr != "" {
		command += " -b " + brokerAddr
	}
	if clusterName != "" {
		command += " -c " + clusterName
	}
	command += " -s true -o true -m false -d false "
	fmt.Println(command)
	out, err := exec.Command("/bin/bash", "-c", command).Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}

func ClusterList(nameserver string) string {
	command := "sh " + root + "/rocketmq-admintools/bin/mqadmin clusterlist"
	if nameserver != "" {
		command += " -n " + nameserver
	}
	fmt.Println(command)
	out, err := exec.Command("/bin/bash", "-c", command).Output()
	if err != nil {
		log.Fatal(err)
	}
	return string(out)
}
