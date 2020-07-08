/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.streamnative.pulsar.handlers.amqp.qpid.core;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;
import java.net.URI;
import java.net.URISyntaxException;


public interface JmsProvider
{
    Connection getConnection(String urlString) throws Exception;

    Queue getTestQueue(String testQueueName) throws NamingException;

    Queue getQueueFromName(Session session, String name) throws JMSException;

    Queue createQueue(Session session, String queueName) throws JMSException;

    Topic getTestTopic(String testQueueName) throws NamingException;

    Topic createTopic(Connection con, String topicName) throws JMSException;

    Topic createTopicOnDirect(Connection con, String topicName) throws JMSException, URISyntaxException;

    Topic createTopicOnFanout(Connection con, String topicName) throws JMSException, URISyntaxException;

    ConnectionBuilder getConnectionBuilder();

    void addGenericConnectionListener(Connection connection, GenericConnectionListener genericConnectionListener);

    URI getConnectedURI(Connection connection);
}
