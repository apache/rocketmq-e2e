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
#include "utils/VerifyUtils.h"
#include "utils/data/collect/DataCollector.h"
#include "utils/data/collect/DataCollectorManager.h"
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <resource/Resource.h>
#include <spdlog/logger.h>
#include <string>
#include <vector>
#include <future>
#include <chrono>
#include <thread>
#include <absl/container/flat_hash_map.h>

extern std::shared_ptr<spdlog::logger> multi_logger;
extern std::shared_ptr<Resource> resource;

std::atomic<int> VerifyUtils::receivedIndex(0);

long long VerifyUtils::getDelayTime(int delayLevel){
    long long delayTime = 0;
    switch (delayLevel) {
        case 1:
            delayTime = 1*1000;
            break;
        case 2:
            delayTime = 5*1000;
            break;
        case 3:
            delayTime = 10*1000;
            break;
        case 4:
            delayTime = 30*1000;
            break;
        case 5:
            delayTime = 1*60*1000;
            break;
        case 6:
            delayTime = 2*60*1000;
            break;
        case 7:
            delayTime = 3*60*1000;
            break;
        case 8:
            delayTime = 4*60*1000;
            break;
        case 9:
            delayTime = 5*60*1000;
            break;
        case 10:
            delayTime = 6*60*1000;
            break;
        case 11:
            delayTime = 7*60*1000;
            break;
        case 12:
            delayTime = 8*60*1000;
            break;
        case 13:
            delayTime = 9*60*1000;
            break;
        case 14:
            delayTime = 10*60*1000;
            break;
        case 15:
            delayTime = 20*60*1000;
            break;
        case 16:
            delayTime = 30*60*1000;
            break;
        case 17:
            delayTime = 1*60*60*1000;
            break;
        case 18:
            delayTime = 2*60*60*1000;
            break;
    }
    return delayTime;
}

std::unordered_map<std::string, long> VerifyUtils::checkDelay(DataCollector<MQMsg>& dequeueMessages, int delayLevel){
    std::unordered_map<std::string, long> map;
    std::vector<MQMsg> receivedMessages = dequeueMessages.getAllData();
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    // 将时间点转换为时间戳（以毫秒为单位）
    std::chrono::seconds duration = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
    long consumeTime = duration.count()*1000L;
    // std::cout<<getDelayTime(delayLevel)<<std::endl;
    for (auto& msg : receivedMessages) {
        long bornTimestamp = msg.getBornTimestamp();
        // std::cout<<consumeTime<<" "<<bornTimestamp<<std::endl;
        if(std::abs((consumeTime-bornTimestamp)-getDelayTime(delayLevel))>5000){
            map.insert(std::make_pair(msg.getMsgId(), consumeTime-bornTimestamp));
        }
    }
    return map;
}

bool VerifyUtils::checkOrder(DataCollector<MQMsg>& dequeueMessages){
    std::vector<MQMsg> receivedMessages = dequeueMessages.getAllData();
    std::unordered_map<std::string, std::vector<MQMsg>> map;
    for (const auto& receivedMessage : receivedMessages) {
        const std::string& shardingKey = std::to_string(std::stoi(receivedMessage.getBody())%2);
        std::vector<MQMsg> messages;
        if (map.find(shardingKey) != map.end()) {
            map[shardingKey].push_back(receivedMessage);
        }else{
            messages.push_back(receivedMessage);
            map[shardingKey] = messages;
        }
    }
    return checkOrderMessage(map);
}

bool async_function(const std::string& topic,const std::string& subExpression, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer){
    std::vector<rocketmq::MQMessageQueue> mqs;
    try {
        pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
        for (auto& mq : mqs) {
            long long offset = pullConsumer->fetchConsumeOffset(mq, true);
            if(offset<0) continue;
            rocketmq::PullResult pullResult = pullConsumer->pull(mq, subExpression, offset, 32);
            switch (pullResult.pullStatus) {
                case rocketmq::FOUND:
                    for (auto& msg : pullResult.msgFoundList) {
                        multi_logger->info("Message: {}", msg.toString());
                    }
                    offset = pullResult.nextBeginOffset;
                    pullConsumer->updateConsumeOffset(mq, offset);
                    break;
                case rocketmq::NO_MATCHED_MSG:
                    break;
                case rocketmq::NO_NEW_MSG:
                    break;
                case rocketmq::OFFSET_ILLEGAL:
                    break;
                default:
                    break;
            }
        }
    } catch (const rocketmq::MQException& e) {
        multi_logger->error("fetchSubscribeMessageQueues exception: {}", e.what());
        return false;
    }
    return true;
}

bool VerifyUtils::tryReceiveOnce(const std::string& topic,const std::string& subExpression, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer){
    // async_function(topic, pullConsumer);
    std::future<bool> future1 = std::async(std::launch::async, [topic,subExpression, pullConsumer](){ return async_function(topic,subExpression,pullConsumer); });
    // std::future<bool> future2 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future3 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future4 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    // std::future<bool> future5 = std::async(std::launch::async, [topic, pullConsumer](){ return async_function(topic, pullConsumer); });
    
    auto status1 = future1.wait_for(std::chrono::seconds(30));
    // auto status2 = future2.wait_for(std::chrono::seconds(30));
    // auto status3 = future3.wait_for(std::chrono::seconds(30));
    // auto status4 = future4.wait_for(std::chrono::seconds(30));
    // auto status5 = future5.wait_for(std::chrono::seconds(30));
    
    if (status1 == std::future_status::ready && future1.get() == true) {
        return true;
    } else {
        return false;
    }
}

std::vector<rocketmq::MQMessageExt> VerifyUtils::fetchMessages(std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer, const std::string& topic) {
    std::vector<rocketmq::MQMessageQueue> mqs;
    pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
    //rocekmq获取队列中所有未消费的消息，首先判断消息数量是不是为1，然后判断消息体是否为空
    for (auto& mq : mqs) {
        long long offset = pullConsumer->fetchConsumeOffset(mq, true);
        if(offset<0) continue;
        rocketmq::PullResult pullResult = pullConsumer->pull(mq, "", offset, 32);
        switch (pullResult.pullStatus) {
            case rocketmq::FOUND:
                for (auto& msg : pullResult.msgFoundList) {
                    msgs.push_back(msg);
                    // std::cout << "msg body: " << msg.getBody() << std::endl;
                }
                offset = pullResult.nextBeginOffset;
                pullConsumer->updateConsumeOffset(mq, offset);
                break;
            case rocketmq::NO_MATCHED_MSG:
                break;
            case rocketmq::NO_NEW_MSG:
                break;
            case rocketmq::OFFSET_ILLEGAL:
                break;
            default:
                break;
        }
    }
    return msgs;
}

std::vector<std::string> VerifyUtils::waitForMessageConsume(DataCollector<std::string>& enqueueMessages,DataCollector<std::string>& dequeueMessages,long long timeoutMills, int consumedTimes){
    multi_logger->info("Set timeout: {}ms",timeoutMills);

    std::vector<std::string> sendMessages = enqueueMessages.getAllData();

    auto currentTime = std::chrono::steady_clock::now();

    while (!sendMessages.empty()) {
        std::vector<std::string> receivedMessagesCopy = dequeueMessages.getAllData();
        sendMessages.erase(std::remove_if(sendMessages.begin(), sendMessages.end(),
                                          [&](const std::string& enqueueMessageId) {
            auto count = std::count_if(receivedMessagesCopy.begin(), receivedMessagesCopy.end(),
                                       [&](const std::string& msg) {
                return msg == enqueueMessageId;
            });
            
            if (count >= consumedTimes) {
                if (count > consumedTimes) {
                    multi_logger->error("More retry messages were consumed than expected (including one original message)"
                              "Except: {}, Actual: {}, MsgId: {}", consumedTimes, count, enqueueMessageId);
                    assert(false);
                }
                return true;
            }
            return false;
        }), sendMessages.end());

        if (sendMessages.empty()) {
            break;
        }

        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - currentTime).count() >= timeoutMills) {
            multi_logger->error("Timeout but not received all send messages");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return sendMessages;
}

std::vector<std::string> VerifyUtils::waitForMessageConsume(DataCollector<std::string>& enqueueMessages,DataCollector<MQMsg>& dequeueMessages,long long timeoutMills, int consumedTimes){
    multi_logger->info("Set timeout: {}ms",timeoutMills);

    std::vector<std::string> sendMessages = enqueueMessages.getAllData();

    auto currentTime = std::chrono::steady_clock::now();

    while (!sendMessages.empty()) {
        std::vector<MQMsg> receivedMessagesCopy = dequeueMessages.getAllData();
        // std::cout << "receivedMessagesCopy size: " << receivedMessagesCopy.size() << std::endl;
        sendMessages.erase(std::remove_if(sendMessages.begin(), sendMessages.end(),
                                          [&](const std::string& enqueueMessageId) {
            auto count = std::count_if(receivedMessagesCopy.begin(), receivedMessagesCopy.end(),
                                       [&](const MQMsg& msg) {
                return msg.getMsgId() == enqueueMessageId;
            });
            
            if (count >= consumedTimes) {
                if (count > consumedTimes) {
                    multi_logger->error("More retry messages were consumed than expected (including one original message)"
                              "Except: {}, Actual: {}, MsgId: {}", consumedTimes, count, enqueueMessageId);
                    assert(false);
                }
                return true;
            }
            return false;
        }), sendMessages.end());

        if (sendMessages.empty()) {
            break;
        }

        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - currentTime).count() >= timeoutMills) {
            multi_logger->error("Timeout but not received all send messages");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return sendMessages;
}

bool VerifyUtils::verifyNormalMessage(DataCollector<std::string>& enqueueMessages, DataCollector<std::string>& dequeueMessages){
    std::vector<std::string> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT*1000L, 1);
    if (unConsumedMessages.size() > 0) {
        multi_logger->error("Not all messages were consumed, unConsumedMessages size: {}", unConsumedMessages.size());
        return false;
    }
    return true;
}

bool VerifyUtils::verifyNormalMessage(DataCollector<std::string>& enqueueMessages, DataCollector<std::string>& dequeueMessages,std::unordered_set<std::string>& unconsumedMsgIds){
    std::vector<std::string> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, 30*1000L, 1);
    for(auto& unConsumedMessage:unConsumedMessages){
        auto it = unconsumedMsgIds.find(unConsumedMessage);
        if(it == unconsumedMsgIds.end()){
            multi_logger->error("Message {} should be consumed",unConsumedMessage);
            return false;
        }else{
            unconsumedMsgIds.erase(it);
        }
    }
    if (unconsumedMsgIds.size() > 0) {
        multi_logger->error("UnConsumedMessages size: {}", unConsumedMessages.size());
        return false;
    }
    return true;
}

bool VerifyUtils::verifyNormalMessage(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages){
    std::vector<std::string> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT*1000L, 1);
    if (unConsumedMessages.size() > 0) {
        multi_logger->error("Not all messages were consumed, unConsumedMessages size: {}", unConsumedMessages.size());
        return false;
    }
    return true;
}

bool VerifyUtils::verifyNormalMessageWithUserProperties(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages,std::map<std::string, std::string>& props,int expectedUnrecvMsgNum){
    std::vector<std::string> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT*1000L, 1);
    std::vector<MQMsg> recvMessages = dequeueMessages.getAllData();
    for(auto& recvMessage:recvMessages){
        auto recvProps = recvMessage.getProperties();
        for (auto& prop : props) {
            auto it = recvProps.find(prop.first);
            if (it != recvProps.end() && it->second == prop.second) {
                multi_logger->error("sql attribute filtering is not in effect, consuming messages to other attributes");
                return false;
            }
        }
    }
    if (unConsumedMessages.size() != expectedUnrecvMsgNum) {
        multi_logger->error("Failed to consume all the sent data by sql filter");
        return false;
    }
    return true;
}

bool VerifyUtils::verifyDelayMessage(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages,int delayLevel){
    std::vector<std::string> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT*1000L+getDelayTime(delayLevel), 1);
    if (unConsumedMessages.size() > 0) {
        multi_logger->error("Not all messages were consumed, unConsumedMessages size: {}", unConsumedMessages.size());
        return false;
    }
    std::unordered_map<std::string, long> delayUnExcept = checkDelay(dequeueMessages, delayLevel);
    std::ostringstream oss;
    oss << "The following messages do not meet the delay requirements \n";
    for(const auto& pair : delayUnExcept){
        std::string key = pair.first;
        oss << key << " , interval:"<< delayUnExcept[key] << "\n";
    }
    if(delayUnExcept.size() > 0){
        multi_logger->error(oss.str());
        return false;
    }
    return true;
}

bool VerifyUtils::verifyOrderMessage(DataCollector<std::string>& enqueueMessages, DataCollector<MQMsg>& dequeueMessages){
    std::vector<std::string> unConsumedMessages = waitForMessageConsume(enqueueMessages, dequeueMessages, TIMEOUT*1000L, 1);
    if (unConsumedMessages.size() > 0) {
        multi_logger->error("Not all messages were consumed, unConsumedMessages size: {}", unConsumedMessages.size());
        return false;
    }

    bool result = checkOrder(dequeueMessages);

    if(!result){
        multi_logger->error("Message out of order");
    }
    return result;
}

bool VerifyUtils::checkOrderMessage(std::unordered_map<std::string, std::vector<MQMsg>>& receivedMessage){
    for(auto& pair : receivedMessage){
        std::ostringstream oss;
        int preNode = -1;
        std::string key = pair.first;
        std::vector<MQMsg> msgs = pair.second;
        std::string tag = msgs[0].getTags();
        for(auto& msg : msgs){
            if(msg.getTags() != tag){
                preNode = -1;
            }
            int curNode = std::stoi(msg.getBody());
            oss << curNode << ",";
            if(preNode > curNode){
                multi_logger->error(oss.str());
                return false;
            }
            preNode = curNode;
        }

    }
    return true;
}

void modifyString2Empty(const std::string &msgId,std::vector<std::string>& msgs,std::mutex& mtx,std::atomic<int>& recvCount) {
    std::lock_guard<std::mutex> lock(mtx);
    for (auto& msg : msgs) {
        if(msgId == msg){
            std::cout << "msg id: " << msg << std::endl;
            msg = "";
            std::cout << "msg id change: " << msg << std::endl;
            recvCount--;
            break;
        }
    }
}

bool VerifyUtils::waitReceiveThenAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+TIMEOUT* 1000L;

    std::vector<std::function<bool()>> runnables;
    std::vector<std::string> sendMsgs=producer->getEnqueueMessages()->getAllData();
    std::atomic<int> recvCount(sendMsgs.size());
    std::mutex mtx;

    for(int i=0;i<defaultSimpleThreadNums;i++){
        runnables.push_back([&](){
            try{
                while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
                    std::vector<rocketmq::MQMessageQueue> mqs;
                    pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
                    for (auto& mq : mqs) {
                        std::unique_lock<std::mutex> lock(mtx);
                        long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                        if(offset<0) continue;
                        rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                        switch (pullResult.pullStatus) {
                            case rocketmq::FOUND:
                                for (auto& msg : pullResult.msgFoundList) {
                                    for (auto& sendMsg : sendMsgs) {
                                        if(msg.getMsgId() == sendMsg){
                                            sendMsg = "";
                                            recvCount--;
                                            offset = pullResult.nextBeginOffset;
                                            pullConsumer->updateConsumeOffset(mq, offset);
                                        }
                                    }
                                }
                                break;
                            case rocketmq::NO_MATCHED_MSG:
                                break;
                            case rocketmq::NO_NEW_MSG:
                                break;
                            case rocketmq::OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                        lock.unlock();
                    }
                    
                    if(recvCount == 0){
                        break;
                    }
                    
                }
                
            }
            catch(const std::exception& e){
                multi_logger->error("{}", e.what());
                return false;
            }
            return true;
        });
    }

    std::vector<std::future<bool>> futures;
    for (const auto& runnable : runnables) {
        futures.push_back(std::async(std::launch::async, runnable));
    }

    // 等待所有函数对象完成并获取结果
    for (auto& future : futures) {
        bool result = future.get();
        if(!result) return false;
    }

    if(recvCount != 0){
        multi_logger->error("Not all messages were consumed, unConsumedMessages size: {}", recvCount);
        return false;
    }else{
        return true;
    }
}

bool VerifyUtils::waitFIFOParamReceiveThenNAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+30* 1000L;

    std::vector<std::function<void()>> runnables;
    std::vector<rocketmq::MQMessageExt> receivedMessage;
    std::mutex mtx;

    for(int i=0;i<4;i++){
        runnables.push_back([&](){
            try{
                while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
                    std::vector<rocketmq::MQMessageQueue> mqs;
                    pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
                    for (auto& mq : mqs) {
                        std::unique_lock<std::mutex> lock(mtx);
                        long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                        if(offset<0) continue;
                        rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                        switch (pullResult.pullStatus) {
                            case rocketmq::FOUND:
                                for (auto& msg : pullResult.msgFoundList) {
                                    receivedMessage.push_back(msg);
                                }
                                break;
                            case rocketmq::NO_MATCHED_MSG:
                                break;
                            case rocketmq::NO_NEW_MSG:
                                break;
                            case rocketmq::OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                        lock.unlock();
                    }
                    
                }
                
            }
            catch(const std::exception& e){
                multi_logger->error("{}", e.what());
            }
        });
    }

    std::vector<std::future<void>> futures;
    for (const auto& runnable : runnables) {
        futures.push_back(std::async(std::launch::async, runnable));
    }

    // 等待所有函数对象完成并获取结果
    for (auto& future : futures) {
        future.get();
    }

    for(auto& msg : receivedMessage){
        int id = std::stoi(msg.getBody());
        if(id>=8){
            multi_logger->error("Consumption out of order, expected :Body=0 Actual :Body={}",id);
            return false;
        }
    }
    return true;
}

bool VerifyUtils::waitFIFOParamReceiveThenAckExceptedLast(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+30* 1000L;

    std::vector<std::function<bool()>> runnables;
    std::vector<rocketmq::MQMessageExt> receivedMessage;
    absl::flat_hash_map<std::string,int> map;

    std::mutex mtx;
    for(int i=0;i<4;i++){
        runnables.push_back([&](){
            try{
                while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
                    std::vector<rocketmq::MQMessageQueue> mqs;
                    pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
                    for (auto& mq : mqs) {
                        std::unique_lock<std::mutex> lock(mtx);
                        long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                        if(offset<0) continue;
                        rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                        switch (pullResult.pullStatus) {
                            case rocketmq::FOUND:
                                for(int j=0;j<pullResult.msgFoundList.size();j++){
                                    std::string msgId = pullResult.msgFoundList[j].getMsgId();
                                    int id = std::stoi(pullResult.msgFoundList[j].getBody());

                                    if(id != 19){
                                        offset+=1;
                                    }
                                    pullConsumer->updateConsumeOffset(mq, offset);

                                    if(map.find(msgId) != map.end()){
                                        map[msgId] = map[msgId]+1;
                                    }else{
                                        map[msgId] = 1;
                                    }
                                }
                                break;
                            case rocketmq::NO_MATCHED_MSG:
                                break;
                            case rocketmq::NO_NEW_MSG:
                                break;
                            case rocketmq::OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                        lock.unlock();
                    }
                    
                    if(map.size() != 20){
                        return false;
                    }
                    int count = 0;
                    for(auto& pair : map){
                        if(pair.second > 1){
                            count += 1;
                        }
                    }

                    if(count > 0 && count != 1){
                        return false;
                    }
                }
            }
            catch(const std::exception& e){
                multi_logger->error("{}", e.what());
            }
            return true;
        });
    }

    std::vector<std::future<bool>> futures;
    for (const auto& runnable : runnables) {
        futures.push_back(std::async(std::launch::async, runnable));
    }

    // 等待所有函数对象完成并获取结果
    for (auto& future : futures) {
        bool result = future.get();
        if(!result) return false;
    }

    return true;
}

bool VerifyUtils::waitFIFOReceiveThenAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+TIMEOUT* 1000L;

    std::vector<std::string> sendCollection=producer->getEnqueueMessages()->getAllData();

    try{
        while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
            std::vector<rocketmq::MQMessageQueue> mqs;
            pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
            for (auto& mq : mqs) {
                std::unordered_map<std::string, std::vector<MQMsg>> receivedMessage;
                long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                if(offset<0) continue;
                rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                switch (pullResult.pullStatus) {
                    case rocketmq::FOUND:
                        for(int j=0;j<pullResult.msgFoundList.size();j++){
                            int id = std::stoi(pullResult.msgFoundList[j].getBody())/20;
                            offset+=1;
                            pullConsumer->updateConsumeOffset(mq, offset);
                            sendCollection.erase(std::remove_if(sendCollection.begin(), sendCollection.end(),
                                                                [&](const std::string& enqueueMessageId) {
                                if(pullResult.msgFoundList[j].getMsgId() == enqueueMessageId){
                                    return true;
                                }
                                return false;
                            }), sendCollection.end());

                            std::string msgId(std::to_string(id));
                            if(receivedMessage.find(msgId) != receivedMessage.end()){
                                receivedMessage[msgId].push_back(MQMsg(pullResult.msgFoundList[j]));
                            }else{
                                std::vector<MQMsg> msgs;
                                msgs.push_back(MQMsg(pullResult.msgFoundList[j]));
                                receivedMessage[msgId] = msgs;
                            }
                        }
                        break;
                    case rocketmq::NO_MATCHED_MSG:
                        break;
                    case rocketmq::NO_NEW_MSG:
                        break;
                    case rocketmq::OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
                if(!checkOrderMessage(receivedMessage)){
                    return false;
                }
            }
            if(sendCollection.size() == 0) break;
        }
        if(sendCollection.size() != 0){
            multi_logger->error("Not all messages were consumed, unConsumedMessages size: {}", sendCollection.size());
            return false;
        }
    }
    catch(const std::exception& e){
        multi_logger->error("{}", e.what());
    }
    return true;
}

bool VerifyUtils::waitAckExceptionReReceiveAck(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+60* 1000L;

    try{
        while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
            std::vector<rocketmq::MQMessageQueue> mqs;
            pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
            for (auto& mq : mqs) {
                long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                if(offset<0) continue;
                rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                switch (pullResult.pullStatus) {
                    case rocketmq::FOUND:
                        for(int j=0;j<pullResult.msgFoundList.size();j++){
                            multi_logger->info("Message: {}", pullResult.msgFoundList[j].toString());
                            std::this_thread::sleep_for(std::chrono::seconds(11));
                            offset+=1;
                            pullConsumer->updateConsumeOffset(mq, offset);
                        }
                        break;
                    case rocketmq::NO_MATCHED_MSG:
                        break;
                    case rocketmq::NO_NEW_MSG:
                        break;
                    case rocketmq::OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            }
        }
    }
    catch(const std::exception& e){
        multi_logger->error("{}", e.what());
        return true;
    }
    return true;
}

 bool VerifyUtils::waitReceiveMaxsizeSync(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+TIMEOUT* 1000L;

    absl::flat_hash_map<std::string,rocketmq::MQMessageExt> map;

    try{
        while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
            std::vector<rocketmq::MQMessageQueue> mqs;
            pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
            for (auto& mq : mqs) {
                long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                if(offset<0) continue;
                rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                switch (pullResult.pullStatus) {
                    case rocketmq::FOUND:
                        for(int j=0;j<pullResult.msgFoundList.size();j++){
                            multi_logger->info("Message: {}", pullResult.msgFoundList[j].toString());
                            std::string msgId = pullResult.msgFoundList[j].getMsgId();
                            if(map.find(msgId) != map.end()){
                                multi_logger->error("Duplicate message");
                                return false;
                            }else{
                                offset+=1;
                                pullConsumer->updateConsumeOffset(mq, offset);
                                map[msgId] = pullResult.msgFoundList[j];
                            }
                        }
                        break;
                    case rocketmq::NO_MATCHED_MSG:
                        break;
                    case rocketmq::NO_NEW_MSG:
                        break;
                    case rocketmq::OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            }
            multi_logger->info("receive {} messages",map.size());
            if(map.size() == 300) break;

        }
        DataCollector<std::string>& dequeueMessages = DataCollectorManager<std::string>::getInstance().fetchListDataCollector(RandomUtils::getStringByUUID());
        for(auto& pair : map){
            dequeueMessages.addData(pair.second.getMsgId());
        }
        if(!VerifyUtils::verifyNormalMessage(*(producer->getEnqueueMessages()), dequeueMessages)){
            return false;
        }
    }
    catch(const std::exception& e){
        multi_logger->error("{}", e.what());
        return false;
    }
    return true;
 }

bool VerifyUtils::waitReceiveMultiNack(std::shared_ptr<RMQNormalProducer> producer, std::shared_ptr<rocketmq::DefaultMQPullConsumer> pullConsumer,std::string &topic,std::string &tag, int maxMessageNum){
    long endTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()+30* 1000L;

    std::vector<std::function<bool()>> runnables;
    absl::flat_hash_map<std::string,rocketmq::MQMessageExt> recvMsgs;
    std::vector<bool> flag(20,true);
    std::unordered_set<std::string> unconsumedMsgIds;

    std::mutex mtx;
    for(int i=0;i<4;i++){
        runnables.push_back([&](){
            
            while(endTime > std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()){
                try{
                    std::vector<rocketmq::MQMessageQueue> mqs;
                    pullConsumer->fetchSubscribeMessageQueues(topic, mqs);
                    for (auto& mq : mqs) {
                        std::unique_lock<std::mutex> lock(mtx);
                        long long offset = pullConsumer->fetchConsumeOffset(mq, false);
                        if(offset<0) continue;
                        rocketmq::PullResult pullResult = pullConsumer->pull(mq, tag, offset, maxMessageNum);
                        switch (pullResult.pullStatus) {
                            case rocketmq::FOUND:
                                for(int j=0;j<pullResult.msgFoundList.size();j++){
                                    int id = std::stoi(pullResult.msgFoundList[j].getBody());
                                    if(id == 19 && flag[19]){
                                        flag[19]=false;
                                        unconsumedMsgIds.insert(pullResult.msgFoundList[j].getMsgId());
                                    }else{
                                        if(id == 19){
                                            unconsumedMsgIds.insert(pullResult.msgFoundList[j].getMsgId());
                                        }else{
                                            multi_logger->info("Message: {}", pullResult.msgFoundList[j].toString());
                                            offset+=1;
                                            pullConsumer->updateConsumeOffset(mq, offset);
                                            if(recvMsgs.find(pullResult.msgFoundList[j].getMsgId()) != recvMsgs.end()){
                                                multi_logger->error("Duplicate message");
                                                return false;
                                            }else{
                                                recvMsgs[pullResult.msgFoundList[j].getMsgId()] = pullResult.msgFoundList[j];
                                            }
                                        }
                                    }
                                }
                                break;
                            case rocketmq::NO_MATCHED_MSG:
                                break;
                            case rocketmq::NO_NEW_MSG:
                                break;
                            case rocketmq::OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                        lock.unlock();
                    }
                }catch(const std::exception& e){
                    multi_logger->error("{}", e.what());
                    return false;
                }
                if(recvMsgs.size() == 20) return false;

            }
            return true;
        });
    }

    std::vector<std::future<bool>> futures;
    for (const auto& runnable : runnables) {
        futures.push_back(std::async(std::launch::async, runnable));
    }

    // 等待所有函数对象完成并获取结果
    for (auto& future : futures) {
        bool res = future.get();
        if(!res) return false;
    }
    DataCollector<std::string>& dequeueMessages = DataCollectorManager<std::string>::getInstance().fetchListDataCollector(RandomUtils::getStringByUUID());
    for(auto& pair : recvMsgs){
        dequeueMessages.addData(pair.second.getMsgId());
    }
    if(!verifyNormalMessage( *(producer->getEnqueueMessages()),dequeueMessages,unconsumedMsgIds)){
        return false;
    }

    return true;
}