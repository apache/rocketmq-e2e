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

func TestOrderSendOrderly(t *testing.T) {
	type args struct {
		testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
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
		t.Run("Send 100 sequential messages synchronously, set 2 Messagegroups, and expect these 100 messages to be sequentially consumed by PushConsumer", func(t *testing.T) {
			var wg sync.WaitGroup
			CreateFIFOTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

			simpleConsumer := BuildSimpleConsumer(tt.args.grpcEndpoint, tt.args.cm, tt.args.msgtag, tt.args.ak, tt.args.sk, tt.args.testTopic)

			// graceful stop simpleConsumer
			defer simpleConsumer.GracefulStop()

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)

			// graceful stop producer
			defer producer.GracefulStop()

			var recvMsgCollector *RecvMsgsCollector
			sendMsgCollector := NewSendMsgsCollector()

			wg.Add(1)

			go func() {
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 10)
				wg.Done()
			}()
			go func() {
				for i := 0; i <= 100; i++ {
					msg := BuildFIFOMessage(tt.args.testTopic, tt.args.body, tt.args.msgtag, tt.args.cm, tt.args.keys)
					SendMessage(producer, msg, sendMsgCollector)
				}
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
		})
	}
}
