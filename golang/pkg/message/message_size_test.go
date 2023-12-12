package rocketmqtest

import (
	"context"
	. "rocketmq-go-e2e/utils"
	"testing"
	"time"
)

// todo Golang中不能发送4M的消息,初步认为是版本问题，后期将继续跟进

func TestNormalMessageSize(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Send normal messages synchronously with the body size of 4M+1, expect send failed",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         RandomString(4*1024*1024 + 1),
			},
		},
		{
			name: "Send normal messages synchronously with the body size of 4M, expect send success",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         RandomString(4 * 1024 * 1024),
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

			msg := BuildNormalMessage(tt.args.testTopic, tt.args.body, tt.args.msgtag, tt.args.keys)

			_, err := producer.Send(context.TODO(), msg)
			if err != nil {
				t.Fail()
				t.Log("Error: ", err)
			}
		})
	}
}

func TestDelayMessageSize(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
		deliveryTimestamp                                                                      int
	}
	tests := []struct {
		name string
		args args
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
				msgtag:            RandomString(8),
				keys:              RandomString(8),
				body:              RandomString(4*1024*1024 + 1),
				deliveryTimestamp: 10,
			},
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
				msgtag:            RandomString(8),
				keys:              RandomString(8),
				body:              RandomString(4 * 1024 * 1024),
				deliveryTimestamp: 10,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CreateDelayTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)
			// graceful stop producer
			defer producer.GracefulStop()

			msg := BuildDelayMessage(tt.args.testTopic, tt.args.body, tt.args.msgtag, time.Duration(tt.args.deliveryTimestamp), tt.args.keys)

			_, err := producer.Send(context.TODO(), msg)
			if err != nil {
				t.Fail()
				t.Log("Error: ", err)
			}
		})
	}
}
