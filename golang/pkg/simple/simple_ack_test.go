package simple

import (
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
)

func TestMessageBodyContent(t *testing.T) {
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
		t.Run("Send 20 normal messages synchronously and expect consume with receive() and ack() messages successful", func(t *testing.T) {
			var (
				wg               sync.WaitGroup
				recvMsgCollector *RecvMsgsCollector
				sendMsgCollector *SendMsgsCollector
			)

			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
			simpleConsumer := BuildSimpleConsumer(tt.args.grpcEndpoint, tt.args.cm, tt.args.msgtag, tt.args.ak, tt.args.sk, tt.args.testTopic)

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)
			// graceful stop producer
			defer producer.GracefulStop()

			wg.Add(1)

			go func() {
				sendMsgCollector = SendNormalMessage(producer, tt.args.testTopic, tt.args.body, tt.args.msgtag, 20, tt.args.keys)
			}()

			go func() {
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 30)
				wg.Done()
			}()
			wg.Wait()
			CheckMsgsWithAll(t, sendMsgCollector, recvMsgCollector)
		})
		t.Run("Send 20 normal messages synchronously. Expect consume with receiveAsync() and ack() messages successful", func(t *testing.T) {
			var (
				wg               sync.WaitGroup
				recvMsgCollector *RecvMsgsCollector
				sendMsgCollector *SendMsgsCollector
			)

			CreateTopic(tt.args.testTopic, "", tt.args.clusterName, tt.args.nameServer)
			simpleConsumer := BuildSimpleConsumer(tt.args.grpcEndpoint, tt.args.cm, tt.args.msgtag, tt.args.ak, tt.args.sk, tt.args.testTopic)

			// new producer instance
			producer := BuildProducer(tt.args.grpcEndpoint, tt.args.ak, tt.args.sk, tt.args.testTopic)
			// graceful stop producer
			defer producer.GracefulStop()

			wg.Add(1)

			go func() {
				sendMsgCollector = SendNormalMessageAsync(producer, tt.args.testTopic, tt.args.body, tt.args.msgtag, 20, tt.args.keys)
			}()

			go func() {
				recvMsgCollector = RecvMessage(simpleConsumer, MaxMessageNum, InvisibleDuration, 30)
				wg.Done()
			}()
			wg.Wait()
			CheckMsgsWithAll(t, sendMsgCollector, recvMsgCollector)
		})
	}
}
