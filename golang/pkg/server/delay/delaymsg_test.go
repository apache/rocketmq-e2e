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

package delay_test

import (
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
	"time"
)

func TestDelaySendMessage(t *testing.T) {
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
		t.Run("Send 10 messages, set the message delivery time to 30 seconds after the current system time, and expect to consume all 10 messages after 30 seconds.", func(t *testing.T) {
			var (
				wg           sync.WaitGroup
				msgCount     = 10
				delaySeconds = 30
			)
			CreateDelayTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

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
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, int64(10+delaySeconds))
				wg.Done()
			}()
			go func() {
				for i := 0; i < msgCount; i++ {
					msg := BuildDelayMessage(tt.args.testTopic, tt.args.body, tt.args.msgtag, time.Duration(delaySeconds), tt.args.keys)
					SendMessage(producer, msg, sendMsgCollector)
				}
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
		})
		t.Run("Send 10 messages, set the message delivery time to 10 seconds before the current system time, the expected timing will not take effect, and all 10 messages can be consumed immediately.", func(t *testing.T) {
			var (
				wg           sync.WaitGroup
				msgCount     = 10
				delaySeconds = 30
			)
			CreateDelayTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

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
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, int64(10+delaySeconds))
				wg.Done()
			}()
			go func() {
				for i := 0; i < msgCount; i++ {
					duration, _ := time.ParseDuration("-10s") // 定义时间差量为10秒前
					timeBefore10s := time.Now().Add(duration)
					elapsedTime := time.Since(timeBefore10s)
					msg := BuildDelayMessage(tt.args.testTopic, "test", tt.args.msgtag, elapsedTime, tt.args.keys)
					SendMessage(producer, msg, sendMsgCollector)
				}
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
		})
		t.Run("Send a message and set the delivery time of the message to 24 hours and 5 seconds from the current system time. The expected message delivery fails.", func(t *testing.T) {
			var (
				wg           sync.WaitGroup
				msgCount     = 10
				delaySeconds = 30
			)
			CreateDelayTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

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
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, int64(10+delaySeconds))
				wg.Done()
			}()
			go func() {
				for i := 0; i < msgCount; i++ {
					duration, _ := time.ParseDuration("24h5s") // 定义时间差量为24小时零5秒后
					msg := BuildDelayMessage(tt.args.testTopic, "test", tt.args.msgtag, duration, tt.args.keys)
					SendMessage(producer, msg, sendMsgCollector)
				}
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
			//todo 添加运行失败，但是不报错的逻辑
		})
	}
}
