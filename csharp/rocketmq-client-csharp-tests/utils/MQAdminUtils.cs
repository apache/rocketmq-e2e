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

using System;
using System.Diagnostics;
using System.IO;

namespace Utils
{
    public static class MQAdminUtils
    {
      
        private static string GetRootPath()
        {
            string projectBasePath = Environment.CurrentDirectory;
            string path = Path.GetDirectoryName(projectBasePath);
            path=Path.GetDirectoryName(path);
            path=Path.GetDirectoryName(path);
            path=Path.GetDirectoryName(path);
            path=Path.GetDirectoryName(path);
            return path;
        }
        

        // Execute a shell command and return its output as a string
        public static string ExecuteShellCommand(string command)
        {
            var process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName =  "/bin/bash", // Use cmd on Windows and bash on other platforms
                    Arguments = $"-c \"{command}\"", // Wrap the command with /c or -c flags
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                }
            };
            process.Start();
            string output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            Console.WriteLine(output);
            return output;
        }
        
    
        public static string CreateTopic(string topicName, string brokerAddr, string clusterName, string nameserver)
        {
            // use absolute path
            string path = GetRootPath();
            string command = "sh " + path + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName;
            if (!string.IsNullOrEmpty(nameserver))
            {
                command += " -n " + nameserver;
            }
            if (!string.IsNullOrEmpty(brokerAddr))
            {
                command += " -b " + brokerAddr;
            }
            if (!string.IsNullOrEmpty(clusterName))
            {
                command += " -c " + clusterName;
            }
            Console.WriteLine(command);
            return ExecuteShellCommand(command);
        }

        public static string CreateDelayTopic(string topicName, string brokerAddr, string clusterName, string nameserver)
        {
            // use absolute path
            string path = GetRootPath();
            string command = "sh " + path + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName;
            if (!string.IsNullOrEmpty(nameserver))
            {
                command += " -n " + nameserver;
            }
            if (!string.IsNullOrEmpty(brokerAddr))
            {
                command += " -b " + brokerAddr;
            }
            if (!string.IsNullOrEmpty(clusterName))
            {
                command += " -c " + clusterName;
            }
            command += " -a " + "+message.type=DELAY";
            return ExecuteShellCommand(command);
        }

        public static string CreateFIFOTopic(string topicName, string brokerAddr, string clusterName, string nameserver)
        {
            // use absolute path
            string path = GetRootPath();
            string command = "sh " + path + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName;
            if (!string.IsNullOrEmpty(nameserver))
            {
                command += " -n " + nameserver;
            }
            if (!string.IsNullOrEmpty(brokerAddr))
            {
                command += " -b " + brokerAddr;
            }
            if (!string.IsNullOrEmpty(clusterName))
            {
                command += " -c " + clusterName;
            }
            command += " -a " + "+message.type=FIFO";
            return ExecuteShellCommand(command);
        }

        public static string CreateTransactionTopic(string topicName, string brokerAddr, string clusterName, string nameserver)
        {
            // use absolute path
            string path = GetRootPath();
            string command = "sh " + path + "/rocketmq-admintools/bin/mqadmin updateTopic -t " + topicName;

            if (!string.IsNullOrEmpty(nameserver))
            {
                command += " -n " + nameserver;
            }
            if (!string.IsNullOrEmpty(brokerAddr))
            {
                command += " -b " + brokerAddr;
            }
            if (!string.IsNullOrEmpty(clusterName))
            {
                command += " -c " + clusterName;
            }
            command += " -a " + "+message.type=TRANSACTION";
            return ExecuteShellCommand(command);
        }

        public static string CreateOrderlyConsumerGroup(string consumerGroup, string brokerAddr, string clusterName, string nameserver)
        {
            // use absolute path
            string path = GetRootPath();
            string command = "sh " + path + "/rocketmq-admintools/bin/mqadmin updateSubGroup -g " + consumerGroup;
            if (!string.IsNullOrEmpty(nameserver))
            {
                command += " -n " + nameserver;
            }
            if (!string.IsNullOrEmpty(brokerAddr))
            {
                command += " -b " + brokerAddr;
            }
            if (!string.IsNullOrEmpty(clusterName))
            {
                command += " -c " + clusterName;
            }
            command += " -s true -o true -m false -d false ";
            return ExecuteShellCommand(command);
        }

        public static string ClusterList(string nameserver)
        {
            string path = GetRootPath();
            string command = "sh " + path + "/rocketmq-admintools/bin/mqadmin clusterlist";
            if (!string.IsNullOrEmpty(nameserver))
            {
                command += " -n " + nameserver;
            }
            return ExecuteShellCommand(command);
        }
       
    }
    
}
