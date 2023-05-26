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
	"math"
	"sort"
	"testing"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/stretchr/testify/assert"
)

// check msg with msgId received only once
func CheckMsgsWithMsgId(t *testing.T, sendMsgsCollector *SendMsgsCollector, recvMsgsCollector *RecvMsgsCollector) {
	assert.Equal(t, len(sendMsgsCollector.MsgIds), len(recvMsgsCollector.MsgIds), "they should be equal")
	sort.Strings(recvMsgsCollector.MsgIds)
	sort.Strings(sendMsgsCollector.MsgIds)
	assert.Equal(t, sendMsgsCollector.MsgIds, recvMsgsCollector.MsgIds, "they should be equal")
}

// check msg with msgId tag keys body received only once
func CheckMsgsWithAll(t *testing.T, sendMsgsCollector *SendMsgsCollector, recvMsgsCollector *RecvMsgsCollector) {
	assert.Equal(t, len(sendMsgsCollector.MsgIds), len(recvMsgsCollector.MsgIds), "they should be equal")
	for i, msg := range sendMsgsCollector.SendMsgs {
		var matchedMsgViews *rmq_client.MessageView
		for j, msgViews := range recvMsgsCollector.RecvMsgViews {
			if sendMsgsCollector.MsgIds[i] == msgViews.GetMessageId() {
				matchedMsgViews = recvMsgsCollector.RecvMsgViews[j]
				break
			}
		}
		assert.Equal(t, sendMsgsCollector.MsgIds[i], matchedMsgViews.GetMessageId(), "they should be equal")
		assert.Equal(t, msg.GetKeys(), matchedMsgViews.GetKeys(), "they should be equal")
		assert.Equal(t, msg.GetTag(), matchedMsgViews.GetTag(), "they should be equal")
		assert.Equal(t, msg.Body, matchedMsgViews.GetBody(), "they should be equal")
	}
}

func CheckDelayMsgsWithMsgId(t *testing.T, sendMsgsCollector *SendMsgsCollector, recvMsgsCollector *RecvMsgsCollector, acceptableVariation float64) {
	CheckMsgsWithMsgId(t, sendMsgsCollector, recvMsgsCollector)
	for i, msg := range sendMsgsCollector.SendMsgs {
		var matchedMsgViews *rmq_client.MessageView
		for j, msgViews := range recvMsgsCollector.RecvMsgViews {
			if sendMsgsCollector.MsgIds[i] == msgViews.GetMessageId() {
				matchedMsgViews = recvMsgsCollector.RecvMsgViews[j]
				break
			}
		}
		assert.Equal(t, msg.GetDeliveryTimestamp(), matchedMsgViews.GetDeliveryTimestamp(), "they should be equal")
		assert.True(t, math.Abs(float64(time.Now().Unix())-float64(msg.GetDeliveryTimestamp().Unix())) < acceptableVariation, "they should be equal")
	}
}

func CheckFIFOMsgsWithMsgId(t *testing.T, sendMsgsCollector *SendMsgsCollector, recvMsgsCollector *RecvMsgsCollector) {
	assert.Equal(t, len(sendMsgsCollector.MsgIds), len(recvMsgsCollector.MsgIds), "they should be equal")
	assert.Equal(t, sendMsgsCollector.MsgIds, recvMsgsCollector.MsgIds, "they should be equal")
}

func CheckTransactionMsgsWithMsgId(t *testing.T, sendMsgsCollector *SendMsgsCollector, recvMsgsCollector *RecvMsgsCollector) {
	CheckMsgsWithMsgId(t, sendMsgsCollector, recvMsgsCollector)
}

func CheckMsgsWithMsgBody(t *testing.T, sendMsgsCollector *SendMsgsCollector, recvMsgsCollector *RecvMsgsCollector) {
	var sendMsg string
	var recvMsg string
	for _, msg := range sendMsgsCollector.SendMsgs {
		sendMsg = string(msg.Body)
	}
	for _, msg1 := range recvMsgsCollector.RecvMsgViews {
		recvMsg = string(msg1.GetBody())
	}

	assert.Equal(t, sendMsg, recvMsg)
}
