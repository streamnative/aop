<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/streamnative/aop/blob/master/LICENSE)


# AMQP on Pulsar (AoP)

AoP stands for AMQP on Pulsar. AoP broker supports AMQP0-9-1 protocol, and is backed by Pulsar.

AoP is implemented as a Pulsar [ProtocolHandler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) with protocol name "amqp"
ProtocolHandler is built as a `nar` file, and will be loaded when Pulsar Broker starting.

## Limitations for AoP

## Get started

In this guide, you will learn how to use the Pulsar broker to serve requests from AMQP client.

### Download Pulsar 

Download [Pulsar 2.5.0](http://pulsar.apache.org/en/download/) binary package `apache-pulsar-2.5.0-bin.tar.gz`. and unzip it.

### Download or Build AoP Plugin

#### Download
This link contains all the AoP releases, please download aop nar file from this link:
https://github.com/streamnative/aop/releases

#### Build from code

1. clone this project from GitHub to your local.

```bash
git clone https://github.com/streamnative/aop.git
cd aop
```

2. build the project.
```bash
mvn clean install -DskipTests
```

3. the nar file can be found at this location.
```bash
./amqp-impl/target/pulsar-protocol-handler-amqp-${version}.nar
```

### Config Pulsar broker to run AoP protocol handler as PluginF

As mentioned above, AoP module is loaded along with Pulsar broker. You need to add configs in Pulsar's config file, such as `broker.conf` or `standalone.conf`.

1. Protocol handler's config

You need to add `messagingProtocols`(default value is null) and  `protocolHandlerDirectory` ( default value is "./protocols"), in Pulsar's config file, such as `broker.conf` or `standalone.conf`
For AoP, value for `messagingProtocols` is `amqp`; value for `protocolHandlerDirectory` is the place of AoP nar file.

e.g.
```access transformers
messagingProtocols=amqp
protocolHandlerDirectory=./protocols
```

2. Set AMQP service listeners

Set AMQP service `listeners`. Note that the hostname value in listeners should be the same as Pulsar broker's `advertisedAddress`.

e.g.
```
amqpListeners=amqp://127.0.0.1:5672
advertisedAddress=127.0.0.1
```

### Run Pulsar broker.

With above 2 configs, you can start your Pulsar broker. You can follow Pulsar's [Get started page](http://pulsar.apache.org/docs/en/standalone/) for more details

```access transformers
cd apache-pulsar-2.5.0
bin/pulsar standalone
```

### Run AMQP Client to verify.



### Other configs.

#### log level config

In Pulsar's [log4j2.yaml config file](https://github.com/apache/pulsar/blob/master/conf/log4j2.yaml), you can set AoP's log level.

e.g.
```
    Logger:
      - name: io.streamnative.pulsar.handlers.amqp
        level: debug
        additivity: false
        AppenderRef:
          - ref: Console
``` 

#### all the AoP configs.

There is also other configs that can be changed and placed into Pulsar broker config file.



## Contribute
If you want to make contributions to AMQP on Pulsar, follow the steps below.

### Prerequisite

1. Install system dependency.

Dependency | Installation guide 
|---|---
Java 8 | https://openjdk.java.net/install/
Maven | https://maven.apache.org/

2. Clone code to your machine. 
    
    ```bash
    git@github.com:streamnative/aop.git
    ```

3. Build the project.
    ```bash
    mvn install -DskipTests
    ```

### Contribution workflow

currently this repo disabled fork, developer all work on this repo.

1. Sync you code remote repository.

    ```bash
    git checkout master
    git pull origin master
    ```

2. Checkout new branch for each PR, Do your change, Commit code changes.

   Because this repo disabled fork, It is recommend that you use a prefix of {your_id} before your_branch.

    ```bash
    git checkout -b ${your_id}/your_branch
    ## ... do the changes ...
    git add [your change files]
    git commit -m "what is done for this change"
    git push origin ${your_id}/your_branch
    ```

3. do the local tests and checks before create an PR in github.

    ```bash
    ## build
    mvn install -DskipTests 
    ## run local check
    mvn checkstyle:check && mvn license:check && mvn spotbugs:check
    ## run tests locally
    mvn test
    ```
    
    If you want to run only part of your tests, try command like this.
    
    ```bash
    ## run all tests of a module: ampq-impl/tests
    mvn test -pl amqp-impl
    ## run all tests in test class
    mvn test -pl amqp-impl -Dtest=WantedTestClass
    ## run a specific test method
    mvn test -pl amqp-impl -Dtest=WantedTestClass#wantedTestMethod 
    ```

4. make sure, pushed your latest change, and then create a PR.
  
    ```bash
    git push origin ${your_id}/your_branch 
    ```
  
  Go back to the main page: https://github.com/streamnative/aop, you should find a reminder in yellow, click it to create a PR.
  
  Or you could go to the link directly like: 
  https://github.com/streamnative/aop/pull/new/${your_id}/your_branch

### Usage Standalone

1. clone this project from GitHub to your local.

```bash
git clone https://github.com/streamnative/aop.git
cd aop
```

2. build the project.
```bash
mvn clean install -DskipTests
```

3. copy the nar package to pulsar protocols directory.
```bash
cp ./amqp-impl/target/pulsar-protocol-handler-amqp-${version}.nar $PULSAR_HOME/protocols/pulsar-protocol-handler-amqp-${version}.nar
```

4. modify pulsar standalone conf
```
# conf file: $PULSAR_HOME/conf/standalone.conf

# modify the default number of namespace bundles to 1
defaultNumberOfNamespaceBundles=1

# add amqp configs
messagingProtocols=amqp
protocolHandlerDirectory=./protocols

amqpListeners=amqp://127.0.0.1:5672
advertisedAddress=127.0.0.1
```

5. start pulsar use standalone mode
```
$PULSAR_HOME/bin/pulsar standalone
```

6. add namespace for vhost
```
# for example, the vhost name is `vhost`
bin/pulsar-admin namespaces create public/vhost1
# set retention for the namespace
bin/pulsar-admin namespaces set-retention -s 100M -t 2d public/vhost1
```

7. use RabbitMQ client test

```
# add RabbitMQ client dependency in your project
<dependency>
  <groupId>com.rabbitmq</groupId>
  <artifactId>amqp-client</artifactId>
  <version>5.8.0</version>
</dependency>
```

```
// Java Code

// create connection
ConnectionFactory connectionFactory = new ConnectionFactory();
connectionFactory.setVirtualHost("vhost1");
connectionFactory.setHost("127.0.0.1");
connectionFactory.setPort(5672);
Connection connection = connectionFactory.newConnection();
Channel channel = connection.createChannel();

String exchange = "ex";
String queue = "qu";

// exchage declare
channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true, false, false, null);

// queue declare and bind
channel.queueDeclare(queue, true, false, false, null);
channel.queueBind(queue, exchange, "");

// publish some messages
for (int i = 0; i < 100; i++) {
    channel.basicPublish(exchange, "", null, ("hello - " + i).getBytes());
}

// consume messages
CountDownLatch countDownLatch = new CountDownLatch(100);
channel.basicConsume(queue, true, new DefaultConsumer(channel) {
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        System.out.println("receive msg: " + new String(body));
        countDownLatch.countDown();
    }
});
countDownLatch.await();

// release resource
channel.close();
connection.close();
```
