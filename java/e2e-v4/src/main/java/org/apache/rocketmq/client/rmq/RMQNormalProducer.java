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

package org.apache.rocketmq.client.rmq;

import org.apache.rocketmq.client.callback.RMQSendCallBack;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.AbstractMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.utils.NameUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class RMQNormalProducer extends AbstractMQProducer {
    private static Logger logger = LoggerFactory.getLogger(RMQNormalProducer.class);
    private DefaultMQProducer producer;

    public RMQNormalProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    public void send(String topic, int messageNum) {
        logger.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessage(topic);
            try {
                SendResult sendResult = producer.send(message);
                logger.info(sendResult.toString());

                MessageExt messageExt = new MessageExt();
                messageExt.setMsgId(sendResult.getMsgId());
                logger.info("{}, index: {}", sendResult, i);
                this.enqueueMessages.addData(messageExt);
            } catch (Exception e) {
                logger.error("DefaultMQProducer send message failed");
            }
        }
        logger.info("Producer send messages finished");
    }

    public void send(String topic, String tag, int messageNum) {
        logger.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessageWithTag(topic, tag);
            try {
                SendResult sendResult = producer.send(message);
                MessageExt messageExt = new MessageExt();
                messageExt.setMsgId(sendResult.getMsgId());
                logger.info("{}, index: {}, tag: {}", sendResult, i, tag);
                this.enqueueMessages.addData(messageExt);
            } catch (Exception e) {
                logger.error("DefaultMQProducer send message failed");
            }
        }
        logger.info("Producer send messages finished");
    }

    /**
     * 向哪些Queue发送顺序消息
     *
     * @param mqs        Queue列表
     * @param messageNum 每个Queue发送的消息数量
     */
    public void sendWithQueue(List<MessageQueue> mqs, int messageNum) {
        String tag = NameUtils.getTagName();
        logger.info("Producer start to send messages");
        for (MessageQueue mq : mqs) {
            for (int i = 0; i < messageNum; i++) {
                Message message = MessageFactory.buildOneMessageWithTagAndBody(mq.getTopic(), tag, String.valueOf(i));
                try {
                    SendResult sendResult = producer.send(message, mq);
                    MessageExt messageExt = new MessageExt();
                    messageExt.setMsgId(sendResult.getMsgId());
                    messageExt.setBody(message.getBody());
                    logger.info("{}, index: {}, tag: {}", sendResult, i, tag);
                    this.enqueueMessages.addData(messageExt);
                } catch (Exception e) {
                    logger.error("DefaultMQProducer send message failed");
                }
            }
        }
        logger.info("Producer send messages finished");
    }

    /**
     * 向哪些Queue发送顺序消息
     *
     * @param mqs        Queue列表
     * @param messageNum 每个Queue发送的消息数量
     */
    public void sendWithQueue(List<MessageQueue> mqs, String tag, int messageNum) {
        logger.info("Producer start to send messages");
        for (MessageQueue mq : mqs) {
            for (int i = 0; i < messageNum; i++) {
                Message message = MessageFactory.buildOneMessageWithTagAndBody(mq.getTopic(), tag, String.valueOf(i));
                try {
                    SendResult sendResult = producer.send(message, mq);
                    MessageExt messageExt = new MessageExt();
                    messageExt.setMsgId(sendResult.getMsgId());
                    messageExt.setBody(message.getBody());
                    logger.info("{}, index: {}, tag: {}", sendResult, i, tag);
                    this.enqueueMessages.addData(messageExt);
                } catch (Exception e) {
                    logger.error("DefaultMQProducer send message failed");
                }
            }
        }
        logger.info("Producer send messages finished");
    }

    /**
     * 向Queue发送顺序消息
     *
     * @param mqs        Queue列表
     * @param messageNum 每个Queue发送的消息数量
     */
    public void sendWithQueue(List<MessageQueue> mqs, int messageNum, String tag) {
        logger.info("Producer start to send messages");
        for (MessageQueue mq : mqs) {
            for (int i = 0; i < messageNum; i++) {
                Message message = MessageFactory.buildOneMessageWithTagAndBody(mq.getTopic(), tag, String.valueOf(i));
                try {
                    SendResult sendResult = producer.send(message, mq);
                    MessageExt messageExt = new MessageExt();
                    messageExt.setMsgId(sendResult.getMsgId());
                    messageExt.setBody(message.getBody());
                    logger.info("{}, index: {}, tag: {}", sendResult, i, tag);
                    this.enqueueMessages.addData(messageExt);
                } catch (Exception e) {
                    logger.error("DefaultMQProducer send message failed");
                }
            }
        }
        logger.info("Producer send messages finished");
    }

    /**
     * 发送指定正常的properties的普通消息l
     *
     * @param topic       topic名称
     * @param tag         标签
     * @param messageBody 消息体
     * @param messageNum  消息条数
     */
    public void sendWithTagAndBody(String topic, String tag, String messageBody, int messageNum) {
        logger.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag, messageBody);
            SendResult sendResult = null;
            try {
                sendResult = producer.send(message);
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info(sendResult.toString());
            this.enqueueMessages.addData((MessageExt) message);
        }
        Assertions.assertEquals(messageNum, enqueueMessages.getAllData().size(), "消息没有全部发送成功！");
        logger.info("Producer send messages finished");
    }

    public void sendAsync(String topic, RMQSendCallBack callBack, int messageNum) {
        logger.info("Producer start to async send messages");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessage(topic);
            MessageExt messageExt = null;
            try {
                producer.send(message, callBack);
                callBack.waitResponse();
                if (callBack.isbSuccessResponse()) {
                    messageExt = new MessageExt();
                    messageExt.setMsgId(callBack.getMessageId());
                    this.enqueueMessages.addData(messageExt);
                }
                if (callBack.isbFailResponse()) {
                    this.enqueueFailedMessages.addData(messageExt);
                }
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Producer async send messages finished");
        if (enqueueFailedMessages.getAllData().size() > 0) {
            logger.warn("send failed messages: {}", enqueueFailedMessages.getAllData());
        }
    }

    //
    // public void sendAsyncWithTagAndBody(String topic, String tag, String
    // messageBody, OnsSendCallBack callBack, int messageNum) {
    // logger.info("Producer start to async send messages");
    // for (int i = 0; i < messageNum; i++) {
    // Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag,
    // messageBody);
    // producer.sendAsync(message, callBack);
    // callBack.waitResponse();
    // if (callBack.isBSuccessResponse()) {
    // this.enqueueMessages.addData(message);
    // }
    // if (callBack.isBFailResponse()) {
    // this.enqueueFailedMessages.addData(message);
    // }
    // }
    // logger.info("Producer async send messages finished");
    // if (enqueueFailedMessages.getAllData().size() > 0) {
    // logger.warn("send failed messages: {}", enqueueFailedMessages.getAllData());
    // }
    // }
    //
    public void sendAsync(String topic, String tag, RMQSendCallBack callBack, int messageNum) {
        logger.info("Producer start to async send messages");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessageWithTag(topic, tag);
            MessageExt messageExt = null;
            try {
                producer.send(message, callBack);
                callBack.waitResponse();
                if (callBack.isbSuccessResponse()) {
                    messageExt = new MessageExt();
                    messageExt.setMsgId(callBack.getMessageId());
                    this.enqueueMessages.addData(messageExt);
                }
                if (callBack.isbFailResponse()) {
                    this.enqueueFailedMessages.addData(messageExt);
                }
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Producer async send messages finished");
        if (enqueueFailedMessages.getAllData().size() > 0) {
            logger.warn("send failed messages: {}", enqueueFailedMessages.getAllData());
        }
    }

    public void sendAsync(Message message, RMQSendCallBack callBack) {
        logger.info("Producer start to async send messages");

        MessageExt messageExt = null;
        try {
            producer.send(message, callBack);
            callBack.waitResponse();
            if (callBack.isbSuccessResponse()) {
                messageExt = new MessageExt();
                messageExt.setMsgId(callBack.getMessageId());
                this.enqueueMessages.addData(messageExt);
            }
            if (callBack.isbFailResponse()) {
                this.enqueueFailedMessages.addData(messageExt);
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Producer async send messages finished");
        if (enqueueFailedMessages.getAllData().size() > 0) {
            logger.warn("send failed messages: {}", enqueueFailedMessages.getAllData());
        }
    }

    public void sendOneWay(String topic, String tag, int messageNum) {
        logger.info("Producer start to OneWay send message");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag, UUID.randomUUID().toString());
            MessageExt messageExt = null;
            try {
                producer.sendOneway(message);
                // this.enqueueMessages.addData(messageExt);
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        logger.info("Producer send messages finished");
    }

    public void sendOnewayWithTagAndBody(String topic, String tag, String messageBody, int messageNum) {
        logger.info("Producer start to OneWay send message");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag, messageBody);
            try {
                producer.sendOneway(message);
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // logger.info("topic: {}, msgId: {}, index: {}, tag: {}, body:{}", topic,
            // message.getMsgID(), i, tag, messageBody);
            this.enqueueMessages.addData((MessageExt) message);
        }
        logger.info("Producer send messages finished");
    }

    /**
     * 发送延迟消息
     *
     * @param topic      topic名称
     * @param delayLevel 延迟时间，单位:秒
     * @param messageNum 消息条数
     */
    public void sendDelay(String topic, int delayLevel, int messageNum) {
        logger.info("Producer start to send delay messages");
        for (int i = 0; i < messageNum; i++) {
            Message message = MessageFactory.buildOneMessageWithDelayTimeLevel(topic, delayLevel);
            SendResult sendResult = null;
            MessageExt messageExt = null;
            try {
                sendResult = producer.send(message);
                logger.info(sendResult.toString());

                messageExt = new MessageExt();
                messageExt.setMsgId(sendResult.getMsgId());
                this.enqueueMessages.addData(messageExt);
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Producer send delay messages finished");
    }

    /// **
    // * 发送延迟消息
    // *
    // * @param topic topic名称
    // * @param delaySecondTime 延迟时间，单位:秒
    // * @param messageNum 消息条数
    // */
    // public void sendDelayWithTag(String topic, String tag, int delaySecondTime,
    /// int messageNum) {
    // logger.info("Producer start to send delay messages");
    // for (int i = 0; i < messageNum; i++) {
    // Message message =
    /// MessageFactory.buildOneMessageWithTagAndStartDeliverTime(topic, tag,
    // System.currentTimeMillis() + delaySecondTime * 1000L);
    // SendResult sendResult = producer.send(message);
    // logger.info(sendResult.toString());
    // this.enqueueMessages.addData(message);
    // }
    // logger.info("Producer send delay messages finished");
    // }
    //
    /// **
    // * 发送定时消息
    // *
    // * @param topic topic名称
    // * @param time 定时消息的时间戳
    // * @param messageNum 消息条数
    // */
    // public void sendTimingWithTag(String topic, String tag, long time, int
    /// messageNum) {
    // logger.info("Producer start to send delay messages");
    // for (int i = 0; i < messageNum; i++) {
    // String deliverTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new
    /// Date(System.currentTimeMillis() + time * 1000L));
    // Message message = MessageFactory.buildOneTimingMessageWithTag(topic, tag,
    /// UUID.randomUUID().toString(), deliverTime);
    // SendResult sendResult = producer.send(message);
    // logger.info(sendResult.toString());
    // this.enqueueMessages.addData(message);
    // }
    // logger.info("Producer send delay messages finished");
    // }
    //
    /// **
    // * 发送定时消息
    // *
    // * @param topic topic名称
    // * @param time 定时消息的时间戳
    // * @param messageNum 消息条数
    // */
    // public void sendTimingWithTagAndBody(String topic, String tag, String body,
    /// long time, int messageNum) {
    // logger.info("Producer start to send delay messages");
    // for (int i = 0; i < messageNum; i++) {
    // String deliverTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new
    /// Date(System.currentTimeMillis() + time * 1000L));
    // Message message = MessageFactory.buildOneTimingMessageWithTag(topic, tag,
    /// body, deliverTime);
    // SendResult sendResult = producer.send(message);
    // logger.info(sendResult.toString());
    // this.enqueueMessages.addData(message);
    // }
    // logger.info("Producer send delay messages finished");
    // }
    //
    /// **
    // * @param topic 发送topic
    // * @param tag 消息tag
    // * @param messageNum 消息数量
    // * @param userProps 消息属性
    // */
    // public void sendWithTagAndUserProps(String topic, String tag, int messageNum,
    /// HashMap<String, String> userProps) {
    // logger.info("Producer start to send messages");
    // for (int i = 0; i < messageNum; i++) {
    // Message message = MessageFactory.buildOneMessageWithTagAndUserProps(topic,
    /// tag, UUID.randomUUID().toString(), userProps);
    // SendResult sendResult = producer.send(message);
    // logger.info(sendResult.toString());
    // this.enqueueMessages.addData(message);
    // }
    // logger.info("Producer send messages finished");
    // }
    //
    /// **
    // * @param topic 发送topic
    // * @param tag 消息tag
    // * @param delaySeconds 消息延迟 单位：s
    // * @param messageNum 消息数量
    // * @param userProps 消息属性
    // */
    // public void sendDelayWithTagAndUserProps(String topic, String tag, int
    /// delaySeconds, int messageNum, HashMap<String, String> userProps) {
    // logger.info("Producer start to send messages");
    // for (int i = 0; i < messageNum; i++) {
    // Message message =
    /// MessageFactory.buildOneDelayMessageWithTagAndUserProps(topic, tag,
    /// System.currentTimeMillis() + delaySeconds * 1000L,
    // UUID.randomUUID().toString(), userProps);
    // SendResult sendResult = producer.send(message);
    // logger.info(sendResult.toString());
    // this.enqueueMessages.addData(message);
    // }
    // logger.info("Producer send messages finished");
    // }

    public void send(String topic, String tag, String body) {
        logger.info("Producer start to send messages");

        Message message = null;
        try {
            message = MessageFactory.buildMessage(topic, tag, body);
        } catch (Exception e) {
            logger.error("build message failed");
        }
        try {
            SendResult sendResult = producer.send(message);
            MessageExt messageExt = new MessageExt();
            messageExt.setMsgId(sendResult.getMsgId());
            logger.info("{}, tag: {}", sendResult, tag);
            this.enqueueMessages.addData(messageExt);
        } catch (Exception e) {
            logger.error("DefaultMQProducer send message failed");
        }

        logger.info("Producer send messages finished");
    }

    public void send(Message message) {
        SendResult sendResult = null;
        MessageExt messageExt = null;
        try {
            sendResult = producer.send(message);
            messageExt = new MessageExt();
            messageExt.setMsgId(sendResult.getMsgId());
            logger.info("{}", sendResult);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.enqueueMessages.addData(messageExt);
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) {
        List<MessageQueue> mqs = null;
        try {
            mqs = producer.fetchPublishMessageQueues(topic);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return mqs;
    }

    @Override
    public void shutdown() {
        if (producer != null) {
            producer.shutdown();
            logger.info("producer shutdown !!!");
        }
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }
}
