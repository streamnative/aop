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

AoP is implemented as a Pulsar [ProtocolHandler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) with protocol name "amqp". ProtocolHandler is built as a `nar` file, and is loaded when Pulsar Broker starts.

## Limitations
AoP is implemented based on Pulsar features. However, the methods of using Pulsar and using AMQP are different. The following are some limitations of AoP.

- Currently, the AoP protocol handler supports AMQP0-9-1 protocol and only supports durable exchange and durable queue.
- A Vhost is backed by a namespace which can only have one bundle. You need to create a namespace in advance for the Vhost.
- AoP is supported on Pulsar 2.6.1 or later releases.

## Get started

In this guide, you will learn how to use the Pulsar broker to serve requests from AMQP client.

### Download Pulsar 

Download [Pulsar 2.6.1](https://github.com/streamnative/pulsar/releases/download/v2.6.1/apache-pulsar-2.6.1-bin.tar.gz) binary package `apache-pulsar-2.6.1-bin.tar.gz`. and unzip it.

### Download and Build AoP Plugin
You can download aop nar file from the [AoP releases](https://github.com/streamnative/aop/releases).

To build from code, complete the following steps:
1. Clone the project from GitHub to your local.

```bash
git clone https://github.com/streamnative/aop.git
cd aop
```

2. Build the project.
```bash
mvn clean install -DskipTests
```
You can find the nar file in the following directory.
```bash
./amqp-impl/target/pulsar-protocol-handler-amqp-${version}.nar
```

### Configuration

|Name|Description|Default|
|---|---|---|
amqpTenant|AMQP on Pulsar broker tenant|public
amqpListeners|AMQP service port|amqp://127.0.0.1:5672
amqpMaxNoOfChannels|The maximum number of channels which can exist concurrently on a connection|64
amqpMaxFrameSize|The maximum frame size on a connection|4194304 (4MB)
amqpHeartBeat|The default heartbeat timeout of AoP connection|60 (s)
amqpProxyPort|The AMQP proxy service port|5682
amqpProxyEnable|Whether to start proxy service|false

### Configure Pulsar broker to run AoP protocol handler as Plugin

As mentioned above, AoP module is loaded with Pulsar broker. You need to add configs in Pulsar's config file, such as `broker.conf` or `standalone.conf`.

1. Protocol handler configuration

You need to add `messagingProtocols`(the default value is `null`) and  `protocolHandlerDirectory` (the default value is "./protocols"), in Pulsar configuration files, such as `broker.conf` or `standalone.conf`. For AoP, the value for `messagingProtocols` is `amqp`; the value for `protocolHandlerDirectory` is the directory of AoP nar file.

The following is an example.
```access transformers
messagingProtocols=amqp
protocolHandlerDirectory=./protocols
```

2. Set AMQP service listeners

Set AMQP service `listeners`. Note that the hostname value in listeners is the same as Pulsar broker's `advertisedAddress`.

The following is an example.
```
amqpListeners=amqp://127.0.0.1:5672
advertisedAddress=127.0.0.1
```

### Run Pulsar broker

With the above configuration, you can start your Pulsar broker. For details, refer to [Pulsar Get started guides](http://pulsar.apache.org/docs/en/standalone/).

```access transformers
cd apache-pulsar-2.6.1
bin/pulsar standalone
```

### Run AMQP Client to verify

### Log level configuration

In Pulsar [log4j2.yaml config file](https://github.com/apache/pulsar/blob/master/conf/log4j2.yaml), you can set AoP log level.

The following is an example.
```
    Logger:
      - name: io.streamnative.pulsar.handlers.amqp
        level: debug
        additivity: false
        AppenderRef:
          - ref: Console
``` 

### AoP configuration

There is also other configs that can be changed and placed into Pulsar broker config file.
<!what's the "other configs"?>

## Contribute
### Prerequisite ###

If you want to make contributions to AMQP on Pulsar, follow the following steps.

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

#### Step 1: Fork

1. Visit https://github.com/streamnative/aop
2. Click `Fork` button (top right) to establish a cloud-based fork.

#### Step 2: Clone fork to local machine

Create your clone.

```sh
$ cd $working_dir
$ git clone https://github.com/$user/aop
```

Set your clone to track upstream repository.

```sh
$ cd $working_dir/aop
$ git remote add upstream https://github.com/streamnative/aop.git
```

Use the `git remote -v` command, you find the output looks as follows:

```
origin    https://github.com/$user/aop.git (fetch)
origin    https://github.com/$user/aop.git (push)
upstream  https://github.com/streamnative/aop (fetch)
upstream  https://github.com/streamnative/aop (push)
```

#### Step 3: Keep your branch in sync

Get your local master up to date.

```sh
$ cd $working_dir/aop
$ git checkout master
$ git fetch upstream
$ git rebase upstream/master
$ git push origin master 
```

#### Step 4: Create your branch

Branch from master.

```sh
$ git checkout -b myfeature
```

#### Step 5: Edit the code

You can now edit the code on the `myfeature` branch.


#### Step 6: Commit

Commit your changes.

```sh
$ git add <filename>
$ git commit -m "$add a comment"
```

Likely you'll go back and edit-build-test in a few cycles. 

The following commands might be helpful for you.

```sh
$ git add <filename> (used to add one file)
git add -A (add all changes, including new/delete/modified files)
git add -a -m "$add a comment" (add and commit modified and deleted files)
git add -u (add modified and deleted files, not include new files)
git add . (add new and modified files, not including deleted files)
```

#### Step 7: Push

When your commit is ready for review (or just to establish an offsite backup of your work), push your branch to your fork on `github.com`:

```sh
$ git push origin myfeature
```

#### Step 8: Create a pull request

1. Visit your fork at https://github.com/$user/aop (replace `$user` obviously).
2. Click the `Compare & pull request` button next to your `myfeature` branch.

#### Step 9: Get a code review

Once you open your pull request, at least two reviewers will participate in reviewing. Those reviewers will conduct a thorough code review, looking for correctness, bugs, opportunities for improvement, documentation and comments, and style.

Commit changes made in response to review comments to the same branch on your fork.

Very small PRs are easy to review. Very large PRs are very difficult to review.

### How to use Pulsar standalone

1. Clone this project from GitHub to your local.

    ```bash
    git clone https://github.com/streamnative/aop.git
    cd aop
    ```

2. Build the project.

    ```bash
    mvn clean install -DskipTests
    ```

3. Copy the nar package to Pulsar protocols directory.

    ```bash
    cp ./amqp-impl/target/pulsar-protocol-handler-amqp-${version}.nar $PULSAR_HOME/protocols/pulsar-protocol-handler-amqp-${version}.nar
    ```

4. Modify Pulsar standalone configuration

    ```
    # conf file: $PULSAR_HOME/conf/standalone.conf
    
    # add amqp configs
    messagingProtocols=amqp
    protocolHandlerDirectory=./protocols
    
    amqpListeners=amqp://127.0.0.1:5672
    advertisedAddress=127.0.0.1
    ```

5. Start Pulsar in standalone mode.

    ```
    $PULSAR_HOME/bin/pulsar standalone
    ```

6. Add namespace for vhost.

    ```
    # for example, the vhost name is `vhost1`
    bin/pulsar-admin namespaces create -b 1 public/vhost1
    # set retention for the namespace
    bin/pulsar-admin namespaces set-retention -s 100M -t 2d public/vhost1
    ```

7. Use RabbitMQ client test

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

### How to use Proxy

To use proxy, complete the following steps. If you do not know some detailed steps, refer to [Deploy a cluster on bare metal](http://pulsar.apache.org/docs/en/deploy-bare-metal/).

1. Prepare ZooKeeper cluster.

2. Initialize cluster metadata.

3. Prepare bookkeeper cluster.

4. Copy the `pulsar-protocol-handler-amqp-${version}.nar` to the `$PULSAR_HOME/protocols` directory.

5. Start broker.

    broker config

    ```yaml
    messagingProtocols=amqp
    protocolHandlerDirectory=./protocols
    brokerServicePort=6651
    amqpListeners=amqp://127.0.0.1:5672
    
    amqpProxyEnable=true
    amqpProxyPort=5682
    ```

6. Reset the number of the namespace public/default to `1`.

    ```shell script
    $PULSAR_HOME/bin/pulsar-admin namespaces delete public/default
    $PULSAR_HOME/bin/pulsar-admin namespaces create -b 1 public/default
    $PULSAR_HOME/bin/pulsar-admin namespaces set-retention -s 100M -t 3d public/default
    ``` 

7. Prepare exchange and queue for test.

    ```
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setVirtualHost("/");
    connectionFactory.setHost("127.0.0.1");
    connectionFactory.setPort(5682);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    String ex = "ex-perf";
    String qu = "qu-perf";
    channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true);
    channel.queueDeclare(qu, true, false, false, null);
    channel.queueBind(qu, ex, qu);
    channel.close();
    connection.close();
    ```

7. Download RabbitMQ perf tool and test. 

    (https://bintray.com/rabbitmq/java-tools/download_file?file_path=perf-test%2F2.11.0%2Frabbitmq-perf-test-2.11.0-bin.tar.gz)

    ```shell script
    $RABBITMQ_PERF_TOOL_HOME/bin/runjava com.rabbitmq.perf.PerfTest -e ex-perf -u qu-perf -r 1000 -h amqp://127.0.0.1:5682 -p
    ```
