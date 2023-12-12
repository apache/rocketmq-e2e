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
package order_test

import (
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
)

func TestFiFOMsg(t *testing.T) {
	t.Parallel()
	var (
		wg               sync.WaitGroup
		recvMsgCollector *RecvMsgsCollector
		sendMsgCollector = NewSendMsgsCollector()
		// receive messages in a loop
		testTopic    = GetTopicName()
		nameServer   = NAMESERVER
		grpcEndpoint = GRPC_ENDPOINT
		clusterName  = CLUSTER_NAME
		ak           = ""
		sk           = ""
		cm           = GetGroupName()
		msgtag       = RandomString(8)
		keys         = RandomString(8)
		msgCount     = 256
	)

	CreateFIFOTopic(testTopic, "", clusterName, nameServer)
	CreateOrderlyConsumerGroup(cm, "", clusterName, nameServer)
	simpleConsumer := BuildSimpleConsumer(grpcEndpoint, cm, msgtag, ak, sk, testTopic)
	// graceful stop simpleConsumer
	defer simpleConsumer.GracefulStop()

	// new producer instance
	producer := BuildProducer(grpcEndpoint, ak, sk, testTopic)
	// graceful stop producer
	defer producer.GracefulStop()

	wg.Add(1)

	go func() {
		recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 60)
		wg.Done()
	}()

	go func() {
		for i := 0; i < msgCount; i++ {
			msg := BuildFIFOMessage(testTopic, "test", msgtag, cm, keys)
			SendMessage(producer, msg, sendMsgCollector)
		}
	}()
	wg.Wait()
	CheckFIFOMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
}
