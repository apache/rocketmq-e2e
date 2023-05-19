package rocketmqtest

import (
	"context"
	"fmt"
	"rocketmq-go-e2e/utils"

	rmq_client "github.com/apache/rocketmq-clients/golang"
)

type BaseInfo struct {
	Endpoint, Topic, Tag, Key, Body, AccessKey, AccessSecret string
}

func NewBaseInfo(endpoint, topic, tag, key, body, ak, sk string) BaseInfo {
	return BaseInfo{
		Endpoint:     endpoint,
		Topic:        topic,
		Tag:          tag,
		Key:          key,
		Body:         body,
		AccessKey:    ak,
		AccessSecret: sk,
	}
}

func (info BaseInfo) SendNormalMessage() error {
	//info.AccessKey, info.AccessSecret是实例的用户名和密码
	producer := utils.BuildProducer(info.Endpoint, info.AccessKey, info.AccessSecret, info.Topic)

	defer producer.GracefulStop()

	msg := &rmq_client.Message{
		// 为当前消息设置 Topic。
		Topic: info.Topic,
		// 消息体。
		Body: []byte(info.Body),
	}

	if info.Key != "" {
		// 设置消息索引键，可根据关键字精确查找某条消息。
		msg.SetKeys(info.Key)
	}

	if info.Tag != "" {
		// 设置消息 Tag，用于消费端根据指定 Tag 过滤消息。
		msg.SetTag(info.Tag)
	}

	// 发送消息，需要关注发送结果，并捕获失败等异常。
	resp, err := producer.Send(context.TODO(), msg)
	if err != nil {
		return err
	}
	for i := 0; i < len(resp); i++ {
		fmt.Printf("%#v\n", resp[i])
	}
	return nil
}
