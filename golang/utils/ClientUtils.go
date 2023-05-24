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

package utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

var (
	// maximum waiting time for receive func
	awaitDuration = time.Second * 5
	// maximum number of messages received at one time
	maxMessageNum int32 = 32
	// invisibleDuration should > 20s
	invisibleDuration = time.Second * 20
	// receive messages in a loop
	GRPC_ENDPOINT = os.Getenv("GRPC_ENDPOINT")
	NAMESERVER    = os.Getenv("NAMESERVER")
	BROKER_ADDR   = os.Getenv("BROKER_ADDR")
	CLUSTER_NAME  = os.Getenv("CLUSTER_NAME")
	ACCESS_KEY    = os.Getenv("ACCESS_KEY")
	SECRET_KEY    = os.Getenv("SECRET_KEY")
)

func init() {
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()
}
func BuildProducerWithDefaultInfo(topic ...string) rmq_client.Producer {

	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: NAMESERVER,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    ACCESS_KEY,
			AccessSecret: SECRET_KEY,
		},
	},
		rmq_client.WithTopics(topic...),
	)
	if err != nil {
		log.Fatal(err)
	}

	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func BuildProducer(nameserver string, ak string, sk string, topic ...string) rmq_client.Producer {

	// new producer instance
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: nameserver,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    ak,
			AccessSecret: sk,
		},
	},
		rmq_client.WithTopics(topic...),
	)
	if err != nil {
		log.Fatal(err)
	}

	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func BuildTransactionProducer(nameserver string, ak string, sk string, checker *rmq_client.TransactionChecker, topics ...string) rmq_client.Producer {

	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: nameserver,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    ak,
			AccessSecret: sk,
		},
	},
		rmq_client.WithTransactionChecker(checker),
		rmq_client.WithTopics(topics...),
	)
	if err != nil {
		log.Fatal(err)
	}
	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func BuildSimpleConsumer(nameserver string, consumerGroup string, filterExpression string, ak string, sk string, topic string) rmq_client.SimpleConsumer {

	simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
		Endpoint:      nameserver,
		ConsumerGroup: consumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    ak,
			AccessSecret: sk,
		},
	},
		rmq_client.WithAwaitDuration(awaitDuration),
		rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			topic: rmq_client.NewFilterExpression(filterExpression),
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
	return simpleConsumer
}

func BuildNormalMessage(topic string, body string, tag string, keys ...string) *rmq_client.Message {
	msg := &rmq_client.Message{
		Topic: topic,
		Body:  []byte(body),
	}
	msg.SetKeys(keys...)
	msg.SetTag(tag)
	return msg
}

func BuildDelayMessage(topic string, body string, tag string, delaySeconds time.Duration, keys ...string) *rmq_client.Message {
	msg := BuildNormalMessage(topic, body, tag, keys...)
	msg.SetDelayTimestamp(time.Now().Add(time.Second * delaySeconds))
	return msg
}

func BuildFIFOMessage(topic string, body string, tag string, consumerGroup string, keys ...string) *rmq_client.Message {
	msg := BuildNormalMessage(topic, body, tag, keys...)
	msg.SetMessageGroup(consumerGroup)
	return msg
}

func SendMessage(producer rmq_client.Producer, message *rmq_client.Message, sendMsgCollector *SendMsgsCollector) {
	resp, err := producer.Send(context.TODO(), message)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(resp); i++ {
		sendMsgCollector.MsgIds = append(sendMsgCollector.MsgIds, resp[i].MessageID)
		sendMsgCollector.SendMsgs = append(sendMsgCollector.SendMsgs, message)
		fmt.Printf("%#v\n", resp[i])
	}
}

func SendMessageAsync(producer rmq_client.Producer, message *rmq_client.Message, sendMsgCollector *SendMsgsCollector) {
	// send message in async
	producer.SendAsync(context.TODO(), message, func(ctx context.Context, resp []*rmq_client.SendReceipt, err error) {
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			sendMsgCollector.MsgIds = append(sendMsgCollector.MsgIds, resp[i].MessageID)
			sendMsgCollector.SendMsgs = append(sendMsgCollector.SendMsgs, message)
			fmt.Printf("%#v\n", resp[i])
		}
	})
}

func SendNormalMessage(producer rmq_client.Producer, topic string, body string, tag string, sendNum int, keys ...string) *SendMsgsCollector {
	sendMsgCollector := NewSendMsgsCollector()
	for i := 0; i < sendNum; i++ {
		// new a message
		msg := BuildNormalMessage(topic, body, tag, keys...)
		// send message in sync
		resp, err := producer.Send(context.TODO(), msg)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			sendMsgCollector.MsgIds = append(sendMsgCollector.MsgIds, resp[i].MessageID)
			sendMsgCollector.SendMsgs = append(sendMsgCollector.SendMsgs, msg)
			fmt.Printf("%#v\n", resp[i])
		}
	}
	return sendMsgCollector
}

func SendTransactionMessage(producer rmq_client.Producer, topic string, body string, tag string, sendNum int, keys ...string) *SendMsgsCollector {
	sendMsgCollector := NewSendMsgsCollector()
	for i := 0; i < sendNum; i++ {
		// new a message
		msg := BuildNormalMessage(topic, body, tag, keys...)
		// send message in sync
		transaction := producer.BeginTransaction()
		resp, err := producer.SendWithTransaction(context.TODO(), msg, transaction)
		if err != nil {
			log.Fatal(err)
		}
		// commit transaction message directly
		err = transaction.Commit()
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			sendMsgCollector.MsgIds = append(sendMsgCollector.MsgIds, resp[i].MessageID)
			sendMsgCollector.SendMsgs = append(sendMsgCollector.SendMsgs, msg)
			fmt.Printf("%#v\n", resp[i])
		}
	}
	return sendMsgCollector
}

func SendNormalMessageAsync(producer rmq_client.Producer, topic string, body string, tag string, sendNum int, keys ...string) *SendMsgsCollector {
	sendMsgCollector := NewSendMsgsCollector()
	// new a message
	msg := BuildNormalMessage(topic, body, tag, keys...)
	for i := 0; i < sendNum; i++ {
		// send message in async
		producer.SendAsync(context.TODO(), msg, func(ctx context.Context, resp []*rmq_client.SendReceipt, err error) {
			if err != nil {
				log.Fatal(err)
			}
			for i := 0; i < len(resp); i++ {
				sendMsgCollector.MsgIds = append(sendMsgCollector.MsgIds, resp[i].MessageID)
				sendMsgCollector.SendMsgs = append(sendMsgCollector.SendMsgs, msg)
				fmt.Printf("%#v\n", resp[i])
			}
		})
	}
	return sendMsgCollector
}

func RecvMessage(simpleConsumer rmq_client.SimpleConsumer, maxMessageNum int32, invisibleDuration time.Duration, pollSeconds int64) *RecvMsgsCollector {
	recvMsgCollector := NewRecvMsgsCollector()
	start := time.Now().Unix()
	for {
		mvs, err := simpleConsumer.Receive(context.TODO(), maxMessageNum, invisibleDuration)
		if err != nil {
			fmt.Println(err)
		}
		// ack message
		for _, mv := range mvs {
			simpleConsumer.Ack(context.TODO(), mv)
			recvMsgCollector.MsgIds = append(recvMsgCollector.MsgIds, mv.GetMessageId())
			recvMsgCollector.RecvMsgViews = append(recvMsgCollector.RecvMsgViews, mv)
			fmt.Println(mv)
		}
		if time.Now().Unix()-start > pollSeconds {
			break
		}
	}
	return recvMsgCollector
}

func RecvMessageWithNum(simpleConsumer rmq_client.SimpleConsumer, maxMessageNum int32, invisibleDuration time.Duration, pollSeconds int64, recvNum int) *RecvMsgsCollector {
	recvMsgCollector := NewRecvMsgsCollector()
	start := time.Now().Unix()
	for {
		mvs, err := simpleConsumer.Receive(context.TODO(), maxMessageNum, invisibleDuration)
		if err != nil {
			fmt.Println(err)
		}
		// ack message
		for _, mv := range mvs {
			simpleConsumer.Ack(context.TODO(), mv)
			recvMsgCollector.MsgIds = append(recvMsgCollector.MsgIds, mv.GetMessageId())
			recvMsgCollector.RecvMsgViews = append(recvMsgCollector.RecvMsgViews, mv)
			fmt.Println(mv)
		}
		if time.Now().Unix()-start > pollSeconds || len(recvMsgCollector.MsgIds) >= recvNum {
			break
		}
	}
	return recvMsgCollector
}
