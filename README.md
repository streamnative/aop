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

