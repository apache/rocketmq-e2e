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

using System.Text;
using Org.Apache.Rocketmq;
using Xunit;
using Xunit.Abstractions;

using Utils;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Rocketmq.Tests
{
    public class FifoMsgTest : BaseTest
    {
        private readonly ITestOutputHelper _output;

        public FifoMsgTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestSendFifoMsgSyncSimpleConsumerRecv()
        {
            List<string> sendMsgIdList = new List<string>();
            List<string> recvMsgIdList = new List<string>();
            string topic = NameUtils.GetTopicName();
            string consumerGroup = NameUtils.GetGroupName();
            string tag = NameUtils.RandomString(8);

            MQAdminUtils.CreateFIFOTopic(topic, null, clusterName, nameserver);
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
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var bytes = Encoding.UTF8.GetBytes("foobar");
                const string messageGroup = "messageGroup1";
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("yourMessageKey-7044358f98fc")
                    .SetMessageGroup(messageGroup)
                    .Build();

                var sendReceipt = await producer.Send(message);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Send fifo message: {message}, message id: {sendReceipt.MessageId}");
                sendMsgIdList.Add(sendReceipt.MessageId);
            }

            DateTime endTime = DateTime.Now.AddSeconds(60);
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
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Ack fifo message: {message}");
                    await simpleConsumer.Ack(message);
                    recvMsgIdList.Add(message.MessageId);
                }
            }

            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIdList);
        }


        [Fact]
        public async Task TestSendFifoMsgSyncSimpleConsumerRecvTwice()
        {

            List<string> sendMsgIdList = new List<string>();
            List<string> recvMsgIdList = new List<string>();
            string topic = NameUtils.GetTopicName();
            string consumerGroup = NameUtils.GetGroupName();
            string tag = NameUtils.RandomString(8);

            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();
            MQAdminUtils.CreateFIFOTopic(topic, null, clusterName, nameserver);

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
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var bytes = Encoding.UTF8.GetBytes("foobar");
                const string messageGroup = "messageGroup2";
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("yourMessageKey-7044358f98fc")
                    .SetMessageGroup(messageGroup)
                    .Build();

                var sendReceipt = await producer.Send(message);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Send fifo message: {message}, message id: {sendReceipt.MessageId}");
                sendMsgIdList.Add(sendReceipt.MessageId);
            }

            DateTime endTime = DateTime.Now.AddSeconds(30);
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
                    if (message.DeliveryAttempt == 2)
                    {
                        await simpleConsumer.Ack(message);
                        _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Ack fifo message: {message}");
                        recvMsgIdList.Add(message.MessageId);
                    }
                    else
                    {
                        _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Recv but not ack fifo message: {message}");
                    }
                }
                await Task.Delay(2000);
            }

            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIdList);
        }
    }
}