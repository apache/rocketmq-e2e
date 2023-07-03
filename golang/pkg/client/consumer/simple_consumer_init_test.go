package consumer

import (
	. "rocketmq-go-e2e/utils"
	"testing"
	"time"

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
				rmq_client.WithAwaitDuration(time.Second*10),
				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
					tt.args.testTopic: rmq_client.NewFilterExpression("*"),
				}),
			)
			if err != nil {
				t.Fail()
				t.Log("Error: ", err)
			}

			defer simpleConsumer.GracefulStop()

			// start simpleConsumer
			err = simpleConsumer.Start()
			if err != nil {
				t.Fail()
				t.Log("Error: ", err)
			}
		})
	}
}
