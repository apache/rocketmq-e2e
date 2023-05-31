package rocketmqtest

import (
	"context"
	"fmt"
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
)

func TestMessageKey(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Message Key equals 16KB, expect send success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(16 * 1024),
				body:         RandomString(64),
			},
		},
		{
			name: "Message Key beyond 16KB, expect send failed",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(16*1024 + 1),
				body:         RandomString(64),
			},
		},
		{
			name: "Message Key contains invisible characters \u0000 , expect send failed",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         "\u0000",
				body:         RandomString(64),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)
			// graceful stop producer
			defer producer.GracefulStop()

			msg := &rmq_client.Message{
				// 为当前消息设置 Topic。
				Topic: tt.args.testTopic,
				// 消息体。
				Body: []byte(tt.args.body),
			}

			if tt.args.keys != "" {
				// 设置消息索引键，可根据关键字精确查找某条消息。
				msg.SetKeys(tt.args.keys)
			}

			if tt.args.msgtag != "" {
				// 设置消息 Tag，用于消费端根据指定 Tag 过滤消息。
				msg.SetTag(tt.args.msgtag)
			}

			// 发送消息，需要关注发送结果，并捕获失败等异常。
			resp, err := producer.Send(context.TODO(), msg)
			if err != nil {
				t.Errorf("failed to send normal message, err:%s", err)
			}
			for i := 0; i < len(resp); i++ {
				fmt.Printf("%#v\n", resp[i])
			}
		})
	}
}

func TestMessageKeyContentWithChinese(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Message key contains Chinese, expect send and consume success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				cm:           GetGroupName(),
				msgtag:       RandomString(8),
				keys:         RandomString(64),
				body:         "中文字符",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			// maximum number of messages received at one time
			var maxMessageNum int32 = 32
			// invisibleDuration should > 20s
			var invisibleDuration = time.Second * 20
			var msgCount = 10

			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
			simpleConsumer := BuildSimpleConsumer(tt.args.grpcEndpoint, tt.args.cm, tt.args.msgtag, tt.args.ak, tt.args.sk, tt.args.testTopic)

			// graceful stop simpleConsumer
			defer simpleConsumer.GracefulStop()

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)
			// graceful stop producer
			defer producer.GracefulStop()

			var recvMsgCollector *RecvMsgsCollector
			var sendMsgCollector *SendMsgsCollector
			wg.Add(1)

			go func() {
				recvMsgCollector = RecvMessage(simpleConsumer, maxMessageNum, invisibleDuration, 10)
				wg.Done()
			}()
			go func() {
				sendMsgCollector = SendNormalMessage(producer, tt.args.testTopic, tt.args.body, tt.args.msgtag, msgCount, tt.args.keys)
			}()
			wg.Wait()

			CheckMsgsWithMsgBody(t, sendMsgCollector, recvMsgCollector)
		})
	}
}

func TestMessageWithMultiKey(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "The message contains multiple keys, expect send and consume success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				cm:           GetGroupName(),
				msgtag:       RandomString(8),
				keys:         RandomString(64),
				body:         RandomString(64),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			// maximum number of messages received at one time
			var maxMessageNum int32 = 32
			// invisibleDuration should > 20s
			var invisibleDuration = time.Second * 20
			var msgCount = 10
			var k1 = RandomString(64)

			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
			simpleConsumer := BuildSimpleConsumer(tt.args.grpcEndpoint, tt.args.cm, tt.args.msgtag, tt.args.ak, tt.args.sk, tt.args.testTopic)

			// graceful stop simpleConsumer
			defer simpleConsumer.GracefulStop()

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)
			// graceful stop producer
			defer producer.GracefulStop()

			msg := &rmq_client.Message{
				// 为当前消息设置 Topic。
				Topic: tt.args.testTopic,
				// 消息体。
				Body: []byte(tt.args.body),
			}

			if tt.args.keys != "" {
				// 设置消息索引键，可根据关键字精确查找某条消息。
				//添加索引键
				msg.SetKeys(tt.args.keys, k1)
			}

			if tt.args.msgtag != "" {
				// 设置消息 Tag，用于消费端根据指定 Tag 过滤消息。
				msg.SetTag(tt.args.msgtag)
			}

			var recvMsgCollector *RecvMsgsCollector
			var sendMsgCollector *SendMsgsCollector
			wg.Add(1)

			go func() {
				recvMsgCollector = RecvMessage(simpleConsumer, maxMessageNum, invisibleDuration, 10)
				wg.Done()
			}()
			go func() {
				sendMsgCollector = SendNormalMessage(producer, tt.args.testTopic, tt.args.body, tt.args.msgtag, msgCount, tt.args.keys)
			}()
			wg.Wait()

			CheckMsgsWithMsgBody(t, sendMsgCollector, recvMsgCollector)
		})
	}
}