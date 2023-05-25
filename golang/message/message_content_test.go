package rocketmqtest

import (
	"context"
	"fmt"
	. "rocketmq-go-e2e/utils"
	"testing"

	rmq_client "github.com/apache/rocketmq-clients/golang"
)

func TestMessageContent(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Send normal message, setting message body with space character, expect consume success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         " ",
			},
		},
		{
			name: "Send normal message, setting message body with chinese character, expect consume success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         "中文字符",
			},
		},
		{
			name: "Send normal message, setting message body with emoji(😱) character, expect consume success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         "😱",
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
