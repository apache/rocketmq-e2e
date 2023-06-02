package utils

import (
	rmq_client "github.com/apache/rocketmq-clients/golang"
)

func CreateDelayMessage(topic, body string) *rmq_client.Message {
	msg := &rmq_client.Message{
		Topic: topic,
		Body:  []byte(body),
	}
	return msg
}
