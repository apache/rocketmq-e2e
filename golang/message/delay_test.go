package rocketmqtest

import (
	"context"
	"fmt"
	. "rocketmq-go-e2e/utils"
	"testing"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
)

func TestSendDelayMassage(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
		deliveryTimestamp                                                                      int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Send delay messages synchronously with the body size of 4M+1, expect send failed",
			args: args{
				testTopic:         GetTopicName(),
				nameServer:        NAMESERVER,
				grpcEndpoint:      GRPC_ENDPOINT,
				clusterName:       CLUSTER_NAME,
				ak:                "",
				sk:                "",
				cm:                GetGroupName(),
				msgtag:            RandomString(8),
				keys:              RandomString(8),
				body:              RandomString(4*1024*1024 + 1),
				deliveryTimestamp: 10,
			},
			want: "",
		},
		{
			name: "Send delay messages synchronously with the body size of 4M, expect send success",
			args: args{
				testTopic:         GetTopicName(),
				nameServer:        NAMESERVER,
				grpcEndpoint:      GRPC_ENDPOINT,
				clusterName:       CLUSTER_NAME,
				ak:                "",
				sk:                "",
				cm:                GetGroupName(),
				msgtag:            RandomString(8),
				keys:              RandomString(8),
				body:              RandomString(4 * 1024 * 1024),
				deliveryTimestamp: 10,
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CreateDelayTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

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

			if tt.args.deliveryTimestamp != 0 {
				// 设置延时时间
				delay := time.Duration(tt.args.deliveryTimestamp) * time.Second
				sendTime := time.Now().Add(delay)
				msg.SetDelayTimestamp(sendTime)
			}

			// 发送消息，需要关注发送结果，并捕获失败等异常。
			resp, err := producer.Send(context.TODO(), msg)
			if err != nil {
				t.Errorf("failed to send delay message, err:%s", err)
			}
			for i := 0; i < len(resp); i++ {
				fmt.Printf("%#v\n", resp[i])
			}
		})
	}
}
