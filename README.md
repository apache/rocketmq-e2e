## Apache RocketMQ E2E 
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

RocketMQ E2E Test  

### Test Case Coverage
* Message Type
  * Normal message
  * Transaction message
  * Order message
  * Delay message
* Producer
  * Sync Send 
  * Async Send
* PushConsumer
* SimpleConsumer
  * Order/Delay/Transaction/Normal
  * Sync receive/Async receive
  * Sync ack/Async ack
* Client init (Producer/PushConsumer/SimpleConsumer)
  * Parameter settings
* Mode
  * Cluster
  * Broadcast
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
mvn clean test -B -Dgroups=smoke -Dcluster=DefaultCluster
```
##### Options
* `ALL_IP` : not required, default is null 
* `cluster`: not required, default `DefaultCluster`
* `groups`: param of junit5, `src/main/java/org/apache/rocketmq/enums/TESTSET.java`
