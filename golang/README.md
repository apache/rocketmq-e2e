## Apache RocketMQ E2E
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

RocketMQ Goland E2E Test

### Test Case Coverage
* Message Type
    * Normal message
    * Transaction message
    * Order message
    * Delay message
* Producer
    * Sync Send
    * Async Send
* **PushConsumer (sdk not accomplished)**
* SimpleConsumer
    * Order/Delay/Transaction/Normal
    * Sync receive/**Async receive  (sdk not accomplished)**
    * Sync ack/Async ack
* Client init（Producer/SimpleConsumer/**PushConsumer (sdk not accomplished)**）
    * Parameter settings
* Model
    * broadcast
    * cluster
* Message
    * Tag
    * Body
    * Key
    * User property
* Filter
    * Tag
    * Sql
* Retry
    * Normal message
    * Order message

#### How to start
```angular2html
#nameserver、endpoint and broker 、clustername was from ENV ALL_IP，You can view the details in common/bin/env.sh
# cd project and run go e2e test case
cd golang && sh bin/run.sh  
```
##### Options
* `ALL_IP` : required, set by GitHub actions
* `cluster`: not required, default `DefaultCluster`

####  how to replace go client sdk version
```angular2html 
to get version xxx
# go get github.com/apache/rocketmq-clients/golang@xxx
to get latest version
# go get -u github.com/apache/rocketmq-clients/golang
