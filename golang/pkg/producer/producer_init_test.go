package producer

import (
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

func TestProducerInit(t *testing.T) {
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
		t.Run("Producer is normally set,expected success", func(t *testing.T) {
			var wg sync.WaitGroup
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
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 10)
				wg.Done()
			}()
			go func() {
				sendMsgCollector = SendNormalMessage(producer, tt.args.testTopic, "test", tt.args.msgtag, 10, tt.args.keys)
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
		})
		t.Run("The NAMESERVER_ADDR setting of the Producer failed, expect failed", func(t *testing.T) {
			var wg sync.WaitGroup
			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)

			simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
				Endpoint:      "https://www.aliyun.com",
				ConsumerGroup: tt.args.cm,
				Credentials: &credentials.SessionCredentials{
					AccessKey:    tt.args.ak,
					AccessSecret: tt.args.sk,
				},
			},
				rmq_client.WithAwaitDuration(time.Second*10),
				rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
					tt.args.testTopic: rmq_client.NewFilterExpression(tt.args.msgtag),
				}),
			)
			if err != nil {
				t.Fail()
				t.Log("Error: ", err)
				return
			}
			// start simpleConsumer
			err = simpleConsumer.Start()
			if err != nil {
				t.Fail()
				t.Log("Error: ", err)
			}

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
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 10)
				wg.Done()

			}()
			go func() {
				sendMsgCollector = SendNormalMessage(producer, tt.args.testTopic, "test", tt.args.msgtag, 10, tt.args.keys)
			}()
			wg.Wait()

			CheckMsgsWithMsgId(t, sendMsgCollector, recvMsgCollector)
		})
	}
}
