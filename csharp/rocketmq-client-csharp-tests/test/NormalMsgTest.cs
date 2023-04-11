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
using System.Collections.Generic;
using System;
using System.Threading.Tasks;

namespace Rocketmq.Tests
{
    public class NormalMsgTest : BaseTest
    {
        private readonly ITestOutputHelper _output;

        public NormalMsgTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestSendNormalMsgSyncSimpleConsumerRecv()
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
            MQAdminUtils.CreateTopic(topic, null, clusterName, nameserver);

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
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("yourMessageKey-7044358f98fc")
                    .Build();

                var sendReceipt = await producer.Send(message);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Send normal message: {message}, message id: {sendReceipt.MessageId}");
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
                    await simpleConsumer.Ack(message);
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Ack normal message: {message}");
                    recvMsgIdList.Add(message.MessageId);
                }
                await Task.Delay(2000);
            }
            sendMsgIdList.Sort();
            recvMsgIdList.Sort();
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIdList);
        }

        [Fact]
        public async Task TestSendNormalMsgSyncSimpleConsumerRecvRetry()
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
            MQAdminUtils.CreateTopic(topic, null, clusterName, nameserver);
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
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    .SetKeys("yourMessageKey-7044358f98fc")
                    .Build();

                var sendReceipt = await producer.Send(message);
                _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Send normal message: {message}, message id: {sendReceipt.MessageId}");
                sendMsgIdList.Add(sendReceipt.MessageId);
            }

            DateTime endTime = DateTime.UtcNow.AddSeconds(90);
            while (true)
            {
                if (DateTime.UtcNow > endTime)
                {
                    _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] Break while loop");
                    break;
                }

                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {
                    _output.WriteLine(
                        $"[{DateTime.UtcNow:HH:mm:ss.fff}] recv normal msg: {message}, message id: {message.MessageId}");
                }
                await Task.Delay(2000);
            }
        }

        // test send with not match message type

    }
}