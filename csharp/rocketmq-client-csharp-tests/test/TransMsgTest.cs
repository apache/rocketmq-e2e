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

using System.Collections.Concurrent;
using System.Text;
using Org.Apache.Rocketmq;
using Xunit;
using Xunit.Abstractions;

using Utils;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace Rocketmq.Tests
{
    public class TransMsgTest : BaseTest
    {

        private readonly ITestOutputHelper _output;

        public TransMsgTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private class TransactionChecker : ITransactionChecker
        {
            private readonly ConcurrentBag<string> _sendMsgIds;
            private readonly bool _isCommit;
            private readonly ITestOutputHelper _output;

            public TransactionChecker(ConcurrentBag<string> sendMsgIds, bool isCommit, ITestOutputHelper output)
            {
                _sendMsgIds = sendMsgIds;
                _isCommit = isCommit;
                _output = output;
            }

            public TransactionResolution Check(MessageView messageView)
            {
                _sendMsgIds.Add(messageView.MessageId);
                var action = _isCommit ? "commit" : "rollback";
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Checker {action} trans msg, messageview={messageView}");
                return _isCommit ? TransactionResolution.Commit : TransactionResolution.Rollback;
            }
        }

        [Fact]
        public async Task TestSendTransHalfCommitMsgSyncSimpleConsumerRecv()
        {
            ConcurrentBag<string> sendMsgIdList = new ConcurrentBag<string>();
            List<string> recvMsgIdList = new List<string>();
            string topic = NameUtils.GetTopicName();
            string consumerGroup = NameUtils.GetGroupName();
            string tag = NameUtils.RandomString(8);

            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            MQAdminUtils.CreateTransactionTopic(topic, null, clusterName, nameserver);
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression(tag) } };
            await using var simpleConsumer = await new SimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetAwaitDuration(TimeSpan.FromSeconds(15))
                .SetSubscriptionExpression(subscription)
                .Build();
            await simpleConsumer.Receive(32, TimeSpan.FromSeconds(15));

            await using var producer = await new Producer.Builder()
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker(sendMsgIdList, true, _output))
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var transaction = producer.BeginTransaction();
                var bytes = Encoding.UTF8.GetBytes("foobar");
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("keyA", "keyB")
                    .Build();

                var sendReceipt = await producer.Send(message, transaction);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Send trans message: {message}, message id: {sendReceipt.MessageId}");
                if (i % 2 == 0)
                {
                    transaction.Commit();
                    sendMsgIdList.Add(sendReceipt.MessageId);
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Commit trans msg, sendReceipt={sendReceipt}");
                }
            }

            DateTime endTime = DateTime.Now.AddSeconds(120);
            while (true)
            {
                if (DateTime.Now > endTime || recvMsgIdList.Count >= sendNum)
                {
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Break while loop");
                    break;
                }
                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {
                    await simpleConsumer.Ack(message);
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Ack trans message: {message}");
                    recvMsgIdList.Add(message.MessageId);
                }
                await Task.Delay(2000);
            }

            var sendMsgIds = sendMsgIdList.ToList();
            sendMsgIds.Sort();
            recvMsgIdList.Sort();
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIds);
        }

        [Fact]
        public async Task TestSendTransCheckRollbackMsgSyncSimpleConsumerRecv()
        {
            ConcurrentBag<string> sendMsgIdList = new ConcurrentBag<string>();
            List<string> recvMsgIdList = new List<string>();
            string topic = NameUtils.GetTopicName();
            string consumerGroup = NameUtils.GetGroupName();
            string tag = NameUtils.RandomString(8);

            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            MQAdminUtils.CreateTransactionTopic(topic, null, clusterName, nameserver);
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression(tag) } };
            await using var simpleConsumer = await new SimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetAwaitDuration(TimeSpan.FromSeconds(15))
                .SetSubscriptionExpression(subscription)
                .Build();
            await using var producer = await new Producer.Builder()
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker(sendMsgIdList, false, _output))
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var transaction = producer.BeginTransaction();
                var bytes = Encoding.UTF8.GetBytes("foobar");
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("keyA", "keyB")
                    .Build();

                var sendReceipt = await producer.Send(message, transaction);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Send trans msg, sendReceipt={sendReceipt}");
                // Commit the transaction.
                if (i % 2 == 0)
                {
                    transaction.Rollback();
                    sendMsgIdList.Add(sendReceipt.MessageId);
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Rollback trans msg, sendReceipt={sendReceipt}");
                }
            }

            DateTime endTime = DateTime.Now.AddSeconds(60);
            while (true)
            {
                if (DateTime.Now > endTime)
                {
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Break while loop");
                    break;
                }
                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {
                    await simpleConsumer.Ack(message);
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Ack trans message: {message}");
                    recvMsgIdList.Add(message.MessageId);
                }
                await Task.Delay(2000);
            }
            Assert.Equal(recvMsgIdList.Count, 0);
        }


        [Fact]
        public async Task TestSendTransCheckRollbackMsgSyncSimpleConsumerRecvRetrys()
        {
            ConcurrentBag<string> sendMsgIdList = new ConcurrentBag<string>();
            List<string> recvMsgIdList = new List<string>();
            string topic = NameUtils.GetTopicName();
            string consumerGroup = NameUtils.GetGroupName();
            string tag = NameUtils.RandomString(8);

            MQAdminUtils.CreateTransactionTopic(topic, null, clusterName, nameserver);
            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression(tag) } };
            await using var simpleConsumer = await new SimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetAwaitDuration(TimeSpan.FromSeconds(15))
                .SetSubscriptionExpression(subscription)
                .Build();
            await simpleConsumer.Receive(32, TimeSpan.FromSeconds(15));

            await using var producer = await new Producer.Builder()
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker(sendMsgIdList, false, _output))
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var transaction = producer.BeginTransaction();
                var bytes = Encoding.UTF8.GetBytes("foobar");
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("keyA", "keyB")
                    .Build();

                var sendReceipt = await producer.Send(message, transaction);
                // Commit the transaction.
                transaction.Commit();
                sendMsgIdList.Add(sendReceipt.MessageId);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Commit trans msg, sendReceipt={sendReceipt}");
            }

            DateTime endTime = DateTime.UtcNow.AddSeconds(120);
            while (true)
            {
                if (DateTime.UtcNow > endTime || recvMsgIdList.Count == sendNum)
                {
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Break while loop");
                    break;
                }

                try
                {
                    var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                    foreach (var message in messageViews)
                    {
                        if (message.DeliveryAttempt > 2)
                        {
                            _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Ack trans message: {message}");
                            await simpleConsumer.Ack(message);
                            recvMsgIdList.Add(message.MessageId);
                        }
                        else
                        {
                            _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Recv but not ack trans msg trans message: {message}");
                        }
                    }
                    await Task.Delay(2000);
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Exception: {ex}");
                }
            }
            var sendMsgIds = sendMsgIdList.ToList();
            sendMsgIds.Sort();
            recvMsgIdList.Sort();
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIds);
        }
    }
}