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

	rmq_client "github.com/apache/rocketmq-clients/golang"
)

func TestSendTransactionMessage(t *testing.T) {
	type args struct {
		testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		args args
	}{
		{
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				cm:           GetGroupName(),
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         RandomString(8),
			},
		},
	}
	for _, tt := range tests {
		t.Run("The synchronous test message is sent normally, expecting success", func(t *testing.T) {
			var (
				recvMsgCollector *RecvMsgsCollector
				sendMsgCollector *SendMsgsCollector
				wg               sync.WaitGroup
			)
			CreateTransactionTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

			simpleConsumer := BuildSimpleConsumer(tt.args.grpcEndpoint, tt.args.cm, tt.args.msgtag, tt.args.ak, tt.args.sk, tt.args.testTopic)

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

			// new producer instance
			producer := BuildTransactionProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, checker, tt.args.testTopic)

			// graceful stop producer
			defer producer.GracefulStop()

			wg.Add(1)

			go func() {
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 10)
				wg.Done()
			}()
			go func() {
				sendMsgCollector = SendTransactionMessage(producer, tt.args.testTopic, "test", tt.args.msgtag, 11, tt.args.keys)
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
		})
	}
}
