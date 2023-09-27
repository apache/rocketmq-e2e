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
     * 生成一条消息，自定义topic，tags，body，检测body为null抛出异常的问题
     *
     * @param topic topic
     * @return message
     */
    public static Message buildMessage(String topic, String tags, String body) throws Exception{
        return new Message(topic, tags, body.getBytes());
    }

    /**
     * 生成一条消息，自定义topic，tags，body
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
     * 生成一条消息，自定义topic及属性
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
     * 生成一条消息，tag不设置， body使用随机字符串
     *
     * @param topic topic
     * @return message
     */
    public static Message buildOneMessage(String topic) {
        return new Message(topic, "", RandomUtils.getStringByUUID().getBytes());
    }

    /**
     * 根据tag生成一条消息， body使用随机字符串
     *
     * @param topic topic
     * @param tag   tag
     * @return message
     */
    public static Message buildOneMessageWithTag(String topic, String tag) {
        return new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
    }

    /**
     * 根据消息体生成一条消息，tag不设置
     *
     * @param topic       topic
     * @param messageBody 消息体
     * @return message
     */
    public static Message buildOneMessageWithBody(String topic, String messageBody) {
        return new Message(topic, null, messageBody.getBytes());
    }

    /**
     * 构建一条指定tag，消息body的message
     *
     * @param topic 发送topic
     * @param tag   发送tag
     * @param body  消息体
     * @return Message
     */
    public static Message buildOneMessageWithTagAndBody(String topic, String tag, String body) {
        return new Message(topic, tag, body.getBytes());
    }

    ///**
    // * 构建指定一条指定properties，消息body的message
    // *
    // * @param topic      发送topic
    // * @param properties 自定义属性
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
     * 延时消息，单位毫秒（ms），在指定延迟时间（当前时间之后）进行投递，例如消息在3秒后投递
     * 定时消息，单位毫秒（ms），在指定时间戳（当前时间之后）进行投递，例如2016-03-07 16:21:00投递。如果被设置成当前时间戳之前的某个时刻，消息将立即被投递给消费者。
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
    //    //发送时间
    //    msg.putUserProperties("sendTime", String.valueOf(System.currentTimeMillis()));
    //    //推送下发消息时间
    //    msg.putUserProperties("deliverTime", String.valueOf(time));
    //    msg.setStartDeliverTime(time);
    //    return msg;
    //}
    //
    ///**
    // * 定时消息，单位毫秒（ms），在指定时间戳（当前时间之后）进行投递，例如2016-03-07 16:21:00投递。如果被设置成当前时间戳之前的某个时刻，消息将立即被投递给消费者。
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
