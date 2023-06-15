package consumer

import (
	"log"
	. "rocketmq-go-e2e/utils"
	"testing"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

func TestSimpleConsumerInit(t *testing.T) {
	type args struct {
		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "SimpleConsumer all parameters are set properly, expect start success",
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
		{
			name: "Without setting 'ClientConfiguration' of the consumer client, expect start failed",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   "",
				grpcEndpoint: "",
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				cm:           GetGroupName(),
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         RandomString(8),
			},
		},
		{
			name: "Without setting 'ConsumerGroup' of the consumer client, expect start failed",
			args: args{
				testTopic:    GetTopicName(),
				nameServer:   NAMESERVER,
				grpcEndpoint: GRPC_ENDPOINT,
				clusterName:  CLUSTER_NAME,
				ak:           "",
				sk:           "",
				cm:           "",
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         RandomString(8),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

			simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
				Endpoint:      tt.args.grpcEndpoint,
				ConsumerGroup: tt.args.cm,
				Credentials: &credentials.SessionCredentials{
					AccessKey:    tt.args.ak,
					AccessSecret: tt.args.sk,
				},
			},
				//rmq_client.WithAwaitDuration(time.Second*10),
				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
					tt.args.testTopic: rmq_client.NewFilterExpression(tt.args.msgtag),
				}),
			)
			if err != nil {
				log.Fatal(err)
			}

			// start simpleConsumer
			err = simpleConsumer.Start()
			if err != nil {
				log.Fatal(err)
			}
		})
	}
}

//func TestNoSubscription(t *testing.T) {
//	type args struct {
//		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
//	}
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			//todo 未设置Subscription依然可以启动成功
//			name: "Without setting 'AwaitDuration' of the consumer client, expect start failed",
//			args: args{
//				testTopic:    GetTopicName(),
//				nameServer:   NAMESERVER,
//				grpcEndpoint: GRPC_ENDPOINT,
//				clusterName:  CLUSTER_NAME,
//				ak:           "",
//				sk:           "",
//				cm:           GetGroupName(),
//				msgtag:       RandomString(8),
//				keys:         RandomString(8),
//				body:         RandomString(8),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
//
//			simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
//				Endpoint:      tt.args.grpcEndpoint,
//				ConsumerGroup: tt.args.cm,
//				Credentials: &credentials.SessionCredentials{
//					AccessKey:    tt.args.ak,
//					AccessSecret: tt.args.sk,
//				},
//			},
//				rmq_client.WithAwaitDuration(time.Second*10),
//				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
//					tt.args.testTopic: rmq_client.NewFilterExpression(tt.args.msgtag),
//				}),
//			)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			// start simpleConsumer
//			err = simpleConsumer.Start()
//			if err != nil {
//				log.Fatal(err)
//			}
//		})
//	}
//}
//
//func TestNoAwaitDuration(t *testing.T) {
//	type args struct {
//		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
//	}
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			//todo 未设置AwaitDuration依然可以启动成功
//			name: "Without setting 'AwaitDuration' of the consumer client, expect start failed",
//			args: args{
//				testTopic:    GetTopicName(),
//				nameServer:   NAMESERVER,
//				grpcEndpoint: GRPC_ENDPOINT,
//				clusterName:  CLUSTER_NAME,
//				ak:           "",
//				sk:           "",
//				cm:           GetGroupName(),
//				msgtag:       RandomString(8),
//				keys:         RandomString(8),
//				body:         RandomString(8),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
//
//			simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
//				Endpoint:      tt.args.grpcEndpoint,
//				ConsumerGroup: tt.args.cm,
//				Credentials: &credentials.SessionCredentials{
//					AccessKey:    tt.args.ak,
//					AccessSecret: tt.args.sk,
//				},
//			},
//				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
//					tt.args.testTopic: rmq_client.NewFilterExpression(tt.args.msgtag),
//				}),
//			)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			// start simpleConsumer
//			err = simpleConsumer.Start()
//			if err != nil {
//				log.Fatal(err)
//			}
//		})
//	}
//}
//
//func TestEmptySubscription(t *testing.T) {
//	type args struct {
//		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
//	}
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			// todo 未通过
//			name: "Error setting 'SubscriptionExpressions' empty of the consumer client, except start failed",
//			args: args{
//				testTopic:    GetTopicName(),
//				nameServer:   NAMESERVER,
//				grpcEndpoint: GRPC_ENDPOINT,
//				clusterName:  CLUSTER_NAME,
//				ak:           "",
//				sk:           "",
//				cm:           GetGroupName(),
//				msgtag:       RandomString(8),
//				keys:         RandomString(8),
//				body:         RandomString(8),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
//
//			simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
//				Endpoint:      tt.args.grpcEndpoint,
//				ConsumerGroup: tt.args.cm,
//				Credentials: &credentials.SessionCredentials{
//					AccessKey:    tt.args.ak,
//					AccessSecret: tt.args.sk,
//				},
//			},
//				rmq_client.WithAwaitDuration(time.Second*10),
//				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{}),
//			)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			// start simpleConsumer
//			err = simpleConsumer.Start()
//			if err != nil {
//				log.Fatal(err)
//			}
//		})
//	}
//}
//
//func TestAwaitDurationIs0s(t *testing.T) {
//	type args struct {
//		name, testTopic, nameServer, grpcEndpoint, clusterName, ak, sk, cm, msgtag, keys, body string
//	}
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			//todo 测试未通过
//			name: "Error setting 'MaxMessageNum=0' of the consumer client, except start failed",
//			args: args{
//				testTopic:    GetTopicName(),
//				nameServer:   NAMESERVER,
//				grpcEndpoint: GRPC_ENDPOINT,
//				clusterName:  CLUSTER_NAME,
//				ak:           "",
//				sk:           "",
//				cm:           GetGroupName(),
//				msgtag:       RandomString(8),
//				keys:         RandomString(8),
//				body:         RandomString(8),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
//
//			simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
//				Endpoint:      tt.args.grpcEndpoint,
//				ConsumerGroup: tt.args.cm,
//				Credentials: &credentials.SessionCredentials{
//					AccessKey:    tt.args.ak,
//					AccessSecret: tt.args.sk,
//				},
//			},
//				rmq_client.WithAwaitDuration(time.Second*0),
//				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
//					tt.args.testTopic: rmq_client.NewFilterExpression(tt.args.msgtag),
//				}),
//			)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			// start simpleConsumer
//			err = simpleConsumer.Start()
//			if err != nil {
//				log.Fatal(err)
//			}
//		})
//	}
//}
