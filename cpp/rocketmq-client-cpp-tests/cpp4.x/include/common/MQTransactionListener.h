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
#pragma once
#include "rocketmq/TransactionListener.h"
#include "spdlog/logger.h"
#include "resource/Resource.h"

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

class CommitMQTransactionListener : public rocketmq::TransactionListener
{
public:
    CommitMQTransactionListener(){};
    ~CommitMQTransactionListener(){};
    rocketmq::LocalTransactionState executeLocalTransaction(const rocketmq::MQMessage &msg, void *arg)
    {
        if (!arg)
        {
            multi_logger->info("CommitMQTransactionListener executeLocalTransaction transactionId:{}, return state: COMMIT_MESAGE", msg.getTransactionId());
            return rocketmq::LocalTransactionState::COMMIT_MESSAGE;
        }
        rocketmq::LocalTransactionState state = (rocketmq::LocalTransactionState)(*(int *)arg % 3);
        multi_logger->info("CommitMQTransactionListener executeLocalTransaction transactionId:{}, return state: {}", msg.getTransactionId(), (*(int *)arg % 3));
        return state;
    }

    rocketmq::LocalTransactionState checkLocalTransaction(const rocketmq::MQMessageExt &msg)
    {
        multi_logger->info("CommitMQTransactionListener checkLocalTransaction transactionId:{}, return state: COMMIT_MESAGE", msg.getTransactionId());
        return rocketmq::LocalTransactionState::COMMIT_MESSAGE;
    }
};

class RollbackMQTransactionListener : public rocketmq::TransactionListener
{
public:
    RollbackMQTransactionListener(){};
    ~RollbackMQTransactionListener(){};
    rocketmq::LocalTransactionState executeLocalTransaction(const rocketmq::MQMessage &msg, void *arg)
    {
        if (!arg)
        {
            multi_logger->info("RollbackMQTransactionListener executeLocalTransaction transactionId:{}, return state: COMMIT_MESAGE", msg.getTransactionId());
            return rocketmq::LocalTransactionState::COMMIT_MESSAGE;
        }
        rocketmq::LocalTransactionState state = (rocketmq::LocalTransactionState)(*(int *)arg % 3);
        multi_logger->info("RollbackMQTransactionListener executeLocalTransaction transactionId:{}, return state: {}", msg.getTransactionId(), (*(int *)arg % 3));
        return state;
    }

    rocketmq::LocalTransactionState checkLocalTransaction(const rocketmq::MQMessageExt &msg)
    {
        multi_logger->info("RollbackMQTransactionListener checkLocalTransaction transactionId:{}, return state: ROLLBACK_MESSAGE", msg.getTransactionId());
        return rocketmq::LocalTransactionState::ROLLBACK_MESSAGE;
    }
};

class UserdefinedMQTransactionListener : public rocketmq::TransactionListener
{
private:
    std::atomic<int> &commitMsgNum;
    std::atomic<int> &rollbackMsgNum;

public:
    UserdefinedMQTransactionListener(std::atomic<int> &commitMsgNum, std::atomic<int> &rollbackMsgNum) : commitMsgNum(commitMsgNum), rollbackMsgNum(rollbackMsgNum){};
    ~UserdefinedMQTransactionListener(){};
    rocketmq::LocalTransactionState executeLocalTransaction(const rocketmq::MQMessage &msg, void *arg)
    {
        if (!arg)
        {
            multi_logger->info("UserdefinedMQTransactionListener executeLocalTransaction transactionId:{} , return state: COMMIT_MESAGE", msg.getTransactionId());
            return rocketmq::LocalTransactionState::COMMIT_MESSAGE;
        }
        rocketmq::LocalTransactionState state = (rocketmq::LocalTransactionState)(*(int *)arg % 3);
        multi_logger->info("UserdefinedMQTransactionListener executeLocalTransaction transactionId:{}, return state: {}", msg.getTransactionId(), (*(int *)arg % 3));
        return state;
    }

    rocketmq::LocalTransactionState checkLocalTransaction(const rocketmq::MQMessageExt &msg)
    {
        int content = std::stoi(msg.getBody());
        if (content % 2 == 0)
        {
            commitMsgNum++;
            multi_logger->info("UserdefinedMQTransactionListener checkLocalTransaction transactionId:{}, message{}, return state: COMMIT_MESAGE", msg.getTransactionId(), msg.toString());
            return rocketmq::LocalTransactionState::COMMIT_MESSAGE;
        }
        else
        {
            rollbackMsgNum++;
            multi_logger->info("UserdefinedMQTransactionListener checkLocalTransaction transactionId:{}, message{}, return state: ROLLBACK_MESSAGE", msg.getTransactionId(), msg.toString());
            return rocketmq::LocalTransactionState::ROLLBACK_MESSAGE;
        }
    }
};