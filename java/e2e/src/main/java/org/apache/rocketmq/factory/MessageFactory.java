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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.util.NameUtils;
import org.apache.rocketmq.util.RandomUtils;

public class MessageFactory extends BaseFactory {

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
     * Generate a message with no tag and a random string for the body
     *
     * @param topic topic
     * @return message
     */
    public static Message buildOneMessage(String topic) {
        return new Message(topic, "", RandomUtils.getStringByUUID().getBytes());
    }

    /**
     * Generate a message with tag and a random string for the body
     *
     * @param topic topic
     * @param tag   tag
     * @return message
     */
    public static Message buildOneMessageWithTag(String topic, String tag) {
        return new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
    }

    /**
     * Generate a message with messageBody and no tag
     *
     * @param topic       topic
     * @param messageBody messageBody
     * @return message
     */
    public static Message buildOneMessageWithBody(String topic, String messageBody) {
        return new Message(topic, null, messageBody.getBytes());
    }

    /**
     * Generate a message with specify messageBody and message
     *
     * @param topic topic
     * @param tag   tag
     * @param body  messageBody
     * @return Message
     */
    public static Message buildOneMessageWithTagAndBody(String topic, String tag, String body) {
        return new Message(topic, tag, body.getBytes());
    }

    ///**
    // * Generate a message with specify messageBody and properties
    // *
    // * @param topic      topic
    // * @param properties user-defined attribute
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
     * Delay message, in milliseconds (ms), delivered after the specified delay time (after the current time), for example, message delivered after 3 seconds
     * Timed messages, in milliseconds (ms), delivered at a specified timestamp (after the current time), such as 2016-03-07 16:21:00. If set to a time before the current timestamp, the message will be delivered to the consumer immediately.
     * @param topic
     * @param tag
     * @param level
     * @return
     */
    public static Message buildOneMessageWithDelayTimeLevel(String topic, String tag, int level) {
        Message msg = new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
        msg.setDelayTimeLevel(level);
        return msg;
    }

    public static List<Message> buildMessages(String topic, int nums) {
        List<Message> messageList = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            messageList.add(new Message(topic, RandomUtils.getStringByUUID().getBytes()));
        }
        return messageList;
    }

    public static List<Message> buildMessages(String topic, String tag, int nums) {
        List<Message> messageList = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            messageList.add(new Message(topic, tag, RandomUtils.getStringByUUID().getBytes()));
        }
        return messageList;
    }

    //
    //public static Message buildOneMessageWithTagAndStartDeliverTime(String topic, String tag, long time) {
    //    Message msg = new Message(topic, tag, RandomUtils.getStringByUUID().getBytes());
    //    //sendTime
    //    msg.putUserProperties("sendTime", String.valueOf(System.currentTimeMillis()));
    //    //deliverTime
    //    msg.putUserProperties("deliverTime", String.valueOf(time));
    //    msg.setStartDeliverTime(time);
    //    return msg;
    //}
    //
    ///**
    // * Timed messages, in milliseconds (ms), delivered at a specified timestamp (after the current time), such as 2016-03-07 16:21:00. If set to a time before the current timestamp, the message will be delivered to the consumer immediately.
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
    //    //sendTime
    //    msg.putUserProperties("sendTime", String.valueOf(System.currentTimeMillis()));
    //    //deliverTime
    //    msg.putUserProperties("deliverTime", String.valueOf(delaySeconds));
    //    msg.setStartDeliverTime(delaySeconds);
    //    for (Map.Entry<String, String> entry : userProps.entrySet()) {
    //        msg.putUserProperties(entry.getKey(), entry.getValue());
    //    }
    //    return msg;
    //}
    public static org.apache.rocketmq.client.apis.message.Message buildMessage(String topic) {
        org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(RandomUtils.getStringByUUID().getBytes(StandardCharsets.UTF_8))
            .setTag(NameUtils.getRandomTagName())
            .build();
        return message;
    }

    public static org.apache.rocketmq.client.apis.message.Message buildMessage(String topic, String tags) {
        org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(RandomUtils.getStringByUUID().getBytes(StandardCharsets.UTF_8))
            .setKeys(RandomUtils.getStringByUUID())
            .setTag(tags)
            .build();
        return message;
    }

    /**
     * build normal message
     *
     * @param topic
     * @param tags
     * @param body
     * @return
     */
    public static org.apache.rocketmq.client.apis.message.Message buildMessage(String topic, String tags, String body) {
        org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .setTag(tags)
            .setKeys(RandomUtils.getStringByUUID())
            .build();
        return message;
    }

    /**
     * build normal message
     *
     * @param topic
     * @param tags
     * @param body
     * @return
     */
    public static org.apache.rocketmq.client.apis.message.Message buildMessageOnlyTag(String topic, String tags,
        String body) {
        org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .setTag(tags)
            .build();
        return message;
    }

    /**
     * build delay message
     *
     * @param topic
     * @param tags
     * @param body
     * @param deliveryTimestamp
     * @return
     */
    public static org.apache.rocketmq.client.apis.message.Message buildDelayMessage(String topic, String tags,
        String body, long deliveryTimestamp) {
        org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .setTag(tags)
            .setKeys(RandomUtils.getStringByUUID())
            .setDeliveryTimestamp(deliveryTimestamp)
            .build();
        return message;
    }

    /**
     * build order message
     *
     * @param topic
     * @param tags
     * @param body
     * @param messageGroup
     * @return
     */
    public static org.apache.rocketmq.client.apis.message.Message buildOrderMessage(String topic, String tags,
        String body, String messageGroup) {
        org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .setTag(tags)
            .setMessageGroup(messageGroup)
            .build();
        return message;
    }

    /**
     * build messages with property
     * body: random string
     * tag: not set
     *
     * @param topic
     * @param property
     * @return Message
     */
    public static org.apache.rocketmq.client.apis.message.Message buildMessageWithProperty(String topic,
        HashMap<String, String> property) {
        MessageBuilder messageBuilder = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(RandomUtils.getStringByUUID().getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> entry : property.entrySet()) {
            messageBuilder.addProperty(entry.getKey(), entry.getValue());
        }
        return messageBuilder.build();
    }

    public static org.apache.rocketmq.client.apis.message.Message buildMessageWithProperty(String topic,
        String messageBody, HashMap<String, String> property) {
        MessageBuilder messageBuilder = provider.newMessageBuilder()
            .setTopic(topic)
            .setBody(messageBody.getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> entry : property.entrySet()) {
            messageBuilder.addProperty(entry.getKey(), entry.getValue());
        }
        return messageBuilder.build();
    }

    public static org.apache.rocketmq.client.apis.message.Message buildOrderMessageWithProperty(String topic,
        String messageBody, String messageGroup, HashMap<String, String> property) {
        MessageBuilder messageBuilder = provider.newMessageBuilder()
            .setTopic(topic)
            .setMessageGroup(messageGroup)
            .setBody(messageBody.getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> entry : property.entrySet()) {
            messageBuilder.addProperty(entry.getKey(), entry.getValue());
        }
        return messageBuilder.build();
    }

    public static org.apache.rocketmq.client.apis.message.Message buildMessageWithProperty(String topic, String tag,
        HashMap<String, String> property, String body, String... key) {
        MessageBuilder messageBuilder = provider.newMessageBuilder()
            .setTopic(topic)
            .setTag(tag)
            .setKeys(key)
            .setBody(body.getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> entry : property.entrySet()) {
            messageBuilder.addProperty(entry.getKey(), entry.getValue());
        }
        return messageBuilder.build();
    }
}
