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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.AbstractMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.factory.MessageFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQTransactionProducer extends AbstractMQProducer {
    private static Logger logger = LoggerFactory.getLogger(RMQTransactionProducer.class);
    private TransactionMQProducer producer = null;
    private String nsAddr = null;

    public RMQTransactionProducer(TransactionMQProducer producer) {
        this.producer = producer;
    }
    //
    // public RMQTransactionProducer(String nsAddr, String topic,
    // TransactionListener transactionListener) {
    // this(nsAddr, topic, false, transactionListener);
    // }

    @Override
    public void shutdown() {
        producer.shutdown();
    }

    public void send(String topic, String tag, int messageNum) {
        logger.info("Producer start to send messages");
        for (int i = 0; i < messageNum; i++) {
            // Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag,
            // String.valueOf(i));
            Message message = MessageFactory.buildOneMessageWithTag(topic, tag);
            SendResult sendResult = null;
            MessageExt messageExt = null;
            try {
                sendResult = producer.send(message);
                messageExt = new MessageExt();
                messageExt.setMsgId(sendResult.getMsgId());
                logger.info("{}, index: {}, tag: {}", sendResult, i, tag);
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
        logger.info("Producer send messages finished");
    }

    public void sendTrans(String topic, String tag, int messageNum) {
        logger.info("Producer start to send transaction messages");
        for (int i = 0; i < messageNum; i++) {
            // Message message = MessageFactory.buildOneMessageWithTagAndBody(topic, tag,
            // String.valueOf(i));
            Message message = MessageFactory.buildNormalMessage(topic, tag, String.valueOf(i));
            SendResult sendResult = null;
            MessageExt messageExt = null;
            try {
                sendResult = producer.sendMessageInTransaction(message, null);
                messageExt = new MessageExt();
                messageExt.setMsgId(sendResult.getMsgId());
                logger.info("{}, index: {}, tag: {}", sendResult, i, tag);
                this.enqueueMessages.addData(messageExt);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
        logger.info("Producer send messages finished");
    }

}
