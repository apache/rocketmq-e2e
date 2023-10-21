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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.factory;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.utils.RandomUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class MessageFactory {

    public static Message getRandomMessage(String topic) {
        return getStringMessage(topic, RandomUtils.getStringByUUID());
    }

    public static Message getStringMessage(String topic, String body) {
        Message msg = new Message(topic, body.getBytes());
        return msg;
    }

    public static Message getStringMessageByTag(String topic, String tags, String body) {
        Message msg = new Message(topic, tags, body.getBytes());
        return msg;
    }

    public static Message getRandomMessageByTag(String topic, String tags) {
        return getStringMessageByTag(topic, tags, RandomUtils.getStringByUUID());
    }

    public static Collection<Message> getRandomMessageList(String topic, int size) {
        List<Message> msgList = new ArrayList<Message>();
        for (int i = 0; i < size; i++) {
            msgList.add(getRandomMessage(topic));
        }
        return msgList;
    }

    public static Collection<Message> getRandomMessageListByTag(String topic, String tags, int size) {
        List<Message> msgList = new ArrayList<Message>();
        for (int i = 0; i < size; i++) {
            msgList.add(getRandomMessageByTag(topic, tags));
        }
        return msgList;
    }

    /**
     * Generate a message, customize the topic, tags, body, detect the body is null throws an exception problem.
     *
     * @param topic topic
     * @return message
     */
    public static Message buildMessage(String topic, String tags, String body) throws Exception{
        return new Message(topic, tags, body.getBytes());
    }

    /**
     * Generate a message, customize the topic, tags, body.
     *
     * @param topic topic
     * @return message
     */
    public static Message buildNormalMessage(String topic, String tags, String body){
        return new Message(topic, tags, body.getBytes());
    }

    public static Message buildMessageWithProperty(String topic, Map<String,String> userProperties, String body){
        Message msg = new Message(topic, body.getBytes());
        for (Map.Entry<String, String> entry : userProperties.entrySet()) {
            msg.putUserProperty(entry.getKey(), entry.getValue());
        }
        return msg;
    }

    /**
     * Generate a message, customize the topic and properties.
     *
     * @param topic topic
     * @return message
     */
    public static Message buildMessageWithProperty(String topic, Map<String,String> userProperties){
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        for (Map.Entry<String, String> entry : userProperties.entrySet()) {
            msg.putUserProperty(entry.getKey(), entry.getValue());
        }
        return msg;
    }

    /**
     * Generate a message with no tag and a random string for the body.
     *
     * @param topic topic
     * @return message
     */
    public static Message buildOneMessage(String topic) {
        return new Message(topic, "", RandomUtils.getStringByUUID().getBytes());
    }

    /**
     * A message is generated from the tag, with the body using a random string.
     *
     * @param topic topic
     * @param tag   tag
     * @return message
     */
    public static Message buildOneMessageWithTag(String topic, String tag) {
        return new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
    }

    /**
     * A message is generated based on the message body. The tag is not set.
     *
     * @param topic       topic
     * @param messageBody message body
     * @return message
     */
    public static Message buildOneMessageWithBody(String topic, String messageBody) {
        return new Message(topic, null, messageBody.getBytes());
    }

    /**
     * Builds a message specifying the tag, message body
     *
     * @param topic topic
     * @param tag   tag
     * @param body  message body
     * @return Message
     */
    public static Message buildOneMessageWithTagAndBody(String topic, String tag, String body) {
        return new Message(topic, tag, body.getBytes());
    }

    ///**
    // * The build specifies a message that specifies properties, the body of the message
    // *
    // * @param topic      topic
    // * @param properties custom properties
    // * @return Message
    // */
    //public static Message buildMessageWithUserProperties(String topic, HashMap<String, String> properties) {
    //    Message msg = new Message(topic, null, RandomUtils.getStringByUUID().getBytes());
    //    for (Map.Entry<String, String> entry : properties.entrySet()) {
    //        msg.putUserProperties(entry.getKey(), entry.getValue());
    //    }
    //    return msg;
    //}
    //
    /**
     * A delayed message, in milliseconds (ms), is delivered after the specified delay time (after the current time), for example, the message is delivered after 3 seconds
     * An order message, in milliseconds (ms), is delivered in a specified timestamp (after the current time), such as 2016-03-07 16:21:00 delivery. If it is set to a time before the current timestamp, the message will be delivered to the consumer immediately.
     *
     * @param topic
     * @param level
     * @return
     */
    public static Message buildOneMessageWithDelayTimeLevel(String topic, int level) {
        Message msg = new Message(topic, "TagA", RandomUtils.getStringByUUID().getBytes());
        msg.setDelayTimeLevel(level);
        return msg;
    }
    //
    //public static Message buildOneMessageWithTagAndStartDeliverTime(String topic, String tag, long time) {
    //    Message msg = new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
    //    //sending time
    //    msg.putUserProperties("sendTime", String.valueOf(System.currentTimeMillis()));
    //    //push message delivery time
    //    msg.putUserProperties("deliverTime", String.valueOf(time));
    //    msg.setStartDeliverTime(time);
    //    return msg;
    //}
    //
    ///**
    // * An order message, in milliseconds (ms), is delivered in a specified timestamp (after the current time), such as 2016-03-07 16:21:00 delivery. If it is set to a time before the current timestamp, the message will be delivered to the consumer immediately.
    // *
    // * @param topic
    // * @param tag
    // * @param msgBody
    // * @param time
    // * @return
    // */
    //public static Message buildOneTimingMessageWithTag(String topic, String tag, String msgBody, String time) {
    //    long timeStamp = 0;
    //    try {
    //        timeStamp = sdf.parse(time).getTime();
    //    } catch (ParseException e) {
    //        e.printStackTrace();
    //    }
    //    Message msg = new Message(topic, tag, msgBody.getBytes());
    //    msg.putUserProperties("sendTime", String.valueOf(System.currentTimeMillis()));
    //    msg.putUserProperties("deliverTime", String.valueOf(time));
    //    msg.setStartDeliverTime(timeStamp);
    //    return msg;
    //}
    //
    ///**
    // * @param topic
    // * @param tag
    // * @param messageBody
    // * @param userProps
    // * @return
    // */
    //public static Message buildOneMessageWithTagAndUserProps(String topic, String tag, String messageBody, HashMap<String, String> userProps) {
    //    Message msg = new Message(topic, tag, messageBody.getBytes());
    //    for (Map.Entry<String, String> entry : userProps.entrySet()) {
    //        msg.putUserProperties(entry.getKey(), entry.getValue());
    //    }
    //    return msg;
    //}
    //
    ///**
    // * @param topic
    // * @param tag
    // * @param delaySeconds
    // * @param messageBody
    // * @param userProps
    // * @return
    // */
    //public static Message buildOneDelayMessageWithTagAndUserProps(String topic, String tag, long delaySeconds, String messageBody,
    //                                                              HashMap<String, String> userProps) {
    //    Message msg = new Message(topic, tag, messageBody.getBytes());
    //    //发送时间
    //    msg.putUserProperties("sendTime", String.valueOf(System.currentTimeMillis()));
    //    //推送下发消息时间
    //    msg.putUserProperties("deliverTime", String.valueOf(delaySeconds));
    //    msg.setStartDeliverTime(delaySeconds);
    //    for (Map.Entry<String, String> entry : userProps.entrySet()) {
    //        msg.putUserProperties(entry.getKey(), entry.getValue());
    //    }
    //    return msg;
    //}

}
