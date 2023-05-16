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

package transaction_test

import (
	"log"
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
)

func TestTransactionMsg(t *testing.T) {
	t.Parallel()
	var (
		wg               sync.WaitGroup
		recvMsgCollector *RecvMsgsCollector
		sendMsgCollector *SendMsgsCollector
		// maximum number of messages received at one time
		maxMessageNum int32 = 32
		// invisibleDuration should > 20s
		invisibleDuration = time.Second * 20
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
		msgCount     = 10
	)

	wg.Add(1)

	CreateTransactionTopic(testTopic, "", clusterName, nameServer)
	simpleConsumer := BuildSimpleConsumer(grpcEndpoint, cm, msgtag, ak, sk, testTopic)
	// graceful stop simpleConsumer
	defer simpleConsumer.GracefulStop()

	// new producer instance
	var checker = &rmq_client.TransactionChecker{
		Check: func(msgView *rmq_client.MessageView) rmq_client.TransactionResolution {
			log.Printf("check transaction message: %v", msgView)
			sendMsgCollector.MsgIds = append(sendMsgCollector.MsgIds, msgView.GetMessageId())
			msg := &rmq_client.Message{
				Topic: msgView.GetTopic(),
				Body:  msgView.GetBody(),
				Tag:   msgView.GetTag(),
			}
			msg.SetKeys(msgView.GetKeys()...)
			//msg.SetMessageGroup(*msgView.GetMessageGroup())
			//msg.SetDelayTimestamp(*msgView.GetDeliveryTimestamp())
			sendMsgCollector.SendMsgs = append(sendMsgCollector.SendMsgs, msg)
			return rmq_client.COMMIT
		},
	}

	producer := BuildTransactionProducer(grpcEndpoint, ak, sk, checker, testTopic)
	// graceful stop producer
	defer producer.GracefulStop()

	go func() {
		recvMsgCollector = RecvMessage(simpleConsumer, maxMessageNum, invisibleDuration, 10)
		wg.Done()
	}()
	go func() {
		sendMsgCollector = SendTransactionMessage(producer, testTopic, "test", msgtag, msgCount, keys)
	}()
	wg.Wait()

	CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
}
