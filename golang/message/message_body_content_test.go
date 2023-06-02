package rocketmqtest

import (
	. "rocketmq-go-e2e/utils"
	"sync"
	"testing"
	"time"
)

func TestMessageBodyContent(t *testing.T) {
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
				cm:           GetGroupName(),
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
				cm:           GetGroupName(),
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
				cm:           GetGroupName(),
				msgtag:       RandomString(8),
				keys:         RandomString(8),
				body:         "😱",
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
