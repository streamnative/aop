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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.qpid.server.model.Protocol;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageEOFException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AmqpManagementFacade
{
    private static final String AMQP_0_X_REPLY_TO_DESTINATION = "ADDR:!response";
    private static final String AMQP_0_X_CONSUMER_REPLY_DESTINATION =
            "ADDR:$management ; {assert : never, node: { type: queue }, link:{name: \"!response\"}}";
    private final String _managementAddress;
    private final Protocol _protocol;

    public AmqpManagementFacade(final Protocol protocol)
    {
        _managementAddress = protocol == Protocol.AMQP_1_0 ? "$management" : "ADDR:$management";
        _protocol = protocol;
    }

    public void createEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        createEntityUsingAmqpManagement(name, session, type, Collections.<String, Object>emptyMap());
    }

    public void createEntityUsingAmqpManagement(final String name,
                                                final Session session,
                                                final String type,
                                                Map<String, Object> attributes)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "CREATE");
        createMessage.setString("name", name);
        createMessage.setString("object-path", name);
        for (Map.Entry<String, Object> entry : attributes.entrySet())
        {
            createMessage.setObject(entry.getKey(), entry.getValue());
        }
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
        producer.close();
    }

    public Map<String, Object> createEntityAndAssertResponse(final String name,
                                            final String type,
                                            final Map<String, Object> attributes,
                                            final Session session)
            throws JMSException
    {
        Destination replyToDestination;
        Destination replyConsumerDestination;
        if (_protocol == Protocol.AMQP_1_0)
        {
            replyToDestination = session.createTemporaryQueue();
            replyConsumerDestination = replyToDestination;
        }
        else
        {
            replyToDestination = session.createQueue(AMQP_0_X_REPLY_TO_DESTINATION);
            replyConsumerDestination = session.createQueue(AMQP_0_X_CONSUMER_REPLY_DESTINATION);
        }

        MessageConsumer consumer = session.createConsumer(replyConsumerDestination);

        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "CREATE");
        createMessage.setString("name", name);
        createMessage.setString("object-path", name);
        createMessage.setJMSReplyTo(replyToDestination);
        for (Map.Entry<String, Object> entry : attributes.entrySet())
        {
            createMessage.setObject(entry.getKey(), entry.getValue());
        }
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
        producer.close();

        return receiveManagementResponse(consumer, replyToDestination, 201);
    }

    private Map<String, Object> receiveManagementResponse(final MessageConsumer consumer,
                                                          final Destination replyToDestination,
                                                          final int responseStatus) throws JMSException
    {
        Message response = consumer.receive(5000);
        try
        {
            if (response != null)
            {
                int statusCode = response.getIntProperty("statusCode");
                if (statusCode == responseStatus)
                {
                    if (response instanceof MapMessage)
                    {
                        MapMessage bodyMap = (MapMessage) response;
                        Map<String, Object> result = new HashMap<>();
                        Enumeration keys = bodyMap.getMapNames();
                        while (keys.hasMoreElements())
                        {
                            final String key = String.valueOf(keys.nextElement());
                            Object value = bodyMap.getObject(key);
                            result.put(key, value);
                        }
                        return result;
                    }
                    else if (response instanceof ObjectMessage)
                    {
                        Object body = ((ObjectMessage) response).getObject();
                        if (body instanceof Map)
                        {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> bodyMap = (Map<String, Object>) body;
                            return new HashMap<>(bodyMap);
                        }
                    }
                    else
                    {
                        return Collections.emptyMap();
                    }
                }
                else
                {
                    throw new OperationUnsuccessfulException(response.getStringProperty("statusDescription"), statusCode);
                }
            }

            throw new IllegalArgumentException("Cannot parse the results from a management response");
        }
        finally
        {
            consumer.close();
            if (_protocol == Protocol.AMQP_1_0)
            {
                ((TemporaryQueue) replyToDestination).delete();
            }
        }
    }

    public Map<String,Object> updateEntityUsingAmqpManagementAndReceiveResponse(final String name,
                                                                  final String type,
                                                                  Map<String, Object> attributes,
                                                                  final Session session)
            throws JMSException
    {

        Destination replyToDestination;
        Destination replyConsumerDestination;
        if (_protocol == Protocol.AMQP_1_0)
        {
            replyToDestination = session.createTemporaryQueue();
            replyConsumerDestination = replyToDestination;
        }
        else
        {
            replyToDestination = session.createQueue(AMQP_0_X_REPLY_TO_DESTINATION);
            replyConsumerDestination = session.createQueue(AMQP_0_X_CONSUMER_REPLY_DESTINATION);
        }

        MessageConsumer consumer = session.createConsumer(replyConsumerDestination);

        updateEntityUsingAmqpManagement(name, type, attributes, replyToDestination, session);

        return receiveManagementResponse(consumer, replyToDestination, 200);
    }


    public void updateEntityUsingAmqpManagement(final String name,
                                                final Session session,
                                                final String type,
                                                Map<String, Object> attributes)
            throws JMSException
    {
        updateEntityUsingAmqpManagement(name, type, attributes, null, session);
    }

    private void updateEntityUsingAmqpManagement(final String name,
                                                 final String type,
                                                 Map<String, Object> attributes,
                                                 Destination replyToDestination,
                                                 final Session session)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "UPDATE");
        createMessage.setStringProperty("index", "object-path");
        createMessage.setStringProperty("key", name);
        createMessage.setJMSReplyTo(replyToDestination);
        for (Map.Entry<String, Object> entry : attributes.entrySet())
        {
            createMessage.setObject(entry.getKey(), entry.getValue());
        }
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
        producer.close();
    }

    public void deleteEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "DELETE");
        createMessage.setStringProperty("index", "object-path");

        createMessage.setStringProperty("key", name);
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
    }

    public Object performOperationUsingAmqpManagement(final String name,
                                                      final String operation,
                                                      final Session session,
                                                      final String type,
                                                      Map<String, Object> arguments)
            throws JMSException
    {
        Destination replyToDestination;
        Destination replyConsumerDestination;
        if (_protocol == Protocol.AMQP_1_0)
        {
            replyToDestination = session.createTemporaryQueue();
            replyConsumerDestination = replyToDestination;
        }
        else
        {
            replyToDestination = session.createQueue(AMQP_0_X_REPLY_TO_DESTINATION);
            replyConsumerDestination = session.createQueue(AMQP_0_X_CONSUMER_REPLY_DESTINATION);
        }

        MessageConsumer consumer = session.createConsumer(replyConsumerDestination);

        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));

        MapMessage opMessage = session.createMapMessage();
        opMessage.setStringProperty("type", type);
        opMessage.setStringProperty("operation", operation);
        opMessage.setStringProperty("index", "object-path");
        opMessage.setJMSReplyTo(replyToDestination);

        opMessage.setStringProperty("key", name);
        for (Map.Entry<String, Object> argument : arguments.entrySet())
        {
            Object value = argument.getValue();
            if (value.getClass().isPrimitive() || value instanceof String)
            {
                opMessage.setObjectProperty(argument.getKey(), value);
            }
            else
            {
                ObjectMapper objectMapper = new ObjectMapper();
                String jsonifiedValue;
                try
                {
                    jsonifiedValue = objectMapper.writeValueAsString(value);
                }
                catch (JsonProcessingException e)
                {
                    throw new IllegalArgumentException(String.format(
                            "Cannot convert the argument '%s' to JSON to meet JMS type restrictions",
                            argument.getKey()));
                }
                opMessage.setObjectProperty(argument.getKey(), jsonifiedValue);
            }
        }

        producer.send(opMessage);
        if (session.getTransacted())
        {
            session.commit();
        }

        Message response = consumer.receive(5000);
        try
        {
            int statusCode = response.getIntProperty("statusCode");
            if (statusCode < 200 || statusCode > 299)
            {
                throw new OperationUnsuccessfulException(response.getStringProperty("statusDescription"), statusCode);
            }
            if (response instanceof StreamMessage)
            {
                StreamMessage bodyStream = (StreamMessage) response;
                List<Object> result = new ArrayList<>();
                boolean done = false;
                do
                {
                    try
                    {
                        result.add(bodyStream.readObject());
                    }
                    catch (MessageEOFException mfe)
                    {
                        // Expected - end of stream
                        done = true;
                    }
                }
                while (!done);
                return result;
            }
            else if (response instanceof MapMessage)
            {
                MapMessage bodyMap = (MapMessage) response;
                Map<String, Object> result = new TreeMap<>();
                Enumeration mapNames = bodyMap.getMapNames();
                while (mapNames.hasMoreElements())
                {
                    String key = (String) mapNames.nextElement();
                    result.put(key, bodyMap.getObject(key));
                }
                return result;
            }
            else if (response instanceof ObjectMessage)
            {
                return ((ObjectMessage) response).getObject();
            }
            else if (response instanceof BytesMessage)
            {
                BytesMessage bytesMessage = (BytesMessage) response;
                if (bytesMessage.getBodyLength() == 0)
                {
                    return null;
                }
                else
                {
                    byte[] buf = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(buf);
                    return buf;
                }
            }
            throw new IllegalArgumentException(
                    "Cannot parse the results from a management operation.  JMS response message : " + response);
        }
        finally
        {
            if (session.getTransacted())
            {
                session.commit();
            }
            consumer.close();
            if(_protocol == Protocol.AMQP_1_0)
            {
                ((TemporaryQueue)replyToDestination).delete();
            }
        }
    }

    public List<Map<String, Object>> managementQueryObjects(final Session session, final String type)
            throws JMSException
    {
        Destination replyToDestination;
        Destination replyConsumerDestination;
        if(_protocol == Protocol.AMQP_1_0)
        {
            replyToDestination = session.createTemporaryQueue();
            replyConsumerDestination = replyToDestination;
        }
        else
        {
            replyToDestination = session.createQueue(AMQP_0_X_REPLY_TO_DESTINATION);
            replyConsumerDestination = session.createQueue(AMQP_0_X_CONSUMER_REPLY_DESTINATION);
        }

        MessageConsumer consumer = session.createConsumer(replyConsumerDestination);
        MapMessage message = session.createMapMessage();
        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "QUERY");
        message.setStringProperty("entityType", type);
        message.setString("attributeNames", "[]");
        message.setJMSReplyTo(replyToDestination);

        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));
        producer.send(message);

        Message response = consumer.receive(5000);
        try
        {
            if (response instanceof MapMessage)
            {
                MapMessage bodyMap = (MapMessage) response;
                List<String> attributeNames = (List<String>) bodyMap.getObject("attributeNames");
                List<List<Object>> attributeValues = (List<List<Object>>) bodyMap.getObject("results");
                return getResultsAsMaps(attributeNames, attributeValues);
            }
            else if (response instanceof ObjectMessage)
            {
                Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    Map<String, ?> bodyMap = (Map<String, ?>) body;
                    List<String> attributeNames = (List<String>) bodyMap.get("attributeNames");
                    List<List<Object>> attributeValues = (List<List<Object>>) bodyMap.get("results");
                    return getResultsAsMaps(attributeNames, attributeValues);
                }
            }
            throw new IllegalArgumentException("Cannot parse the results from a management query");
        }
        finally
        {
            consumer.close();
            if(_protocol == Protocol.AMQP_1_0)
            {
                ((TemporaryQueue)replyToDestination).delete();
            }
        }
    }

    public Map<String, Object> readEntityUsingAmqpManagement(final Session session,
                                                             final String type,
                                                             final String name,
                                                             final boolean actuals) throws JMSException
    {
        Destination replyToDestination;
        Destination replyConsumerDestination;
        if(_protocol == Protocol.AMQP_1_0)
        {
            replyToDestination = session.createTemporaryQueue();
            replyConsumerDestination = replyToDestination;
        }
        else
        {
            replyToDestination = session.createQueue(AMQP_0_X_REPLY_TO_DESTINATION);
            replyConsumerDestination = session.createQueue(AMQP_0_X_CONSUMER_REPLY_DESTINATION);
        }

        MessageConsumer consumer = session.createConsumer(replyConsumerDestination);

        MessageProducer producer = session.createProducer(session.createQueue(_managementAddress));


        MapMessage request = session.createMapMessage();
        request.setStringProperty("type", type);
        request.setStringProperty("operation", "READ");
        request.setString("name", name);
        request.setString("object-path", name);
        request.setStringProperty("index", "object-path");
        request.setStringProperty("key", name);
        request.setBooleanProperty("actuals", actuals);
        request.setJMSReplyTo(replyToDestination);

        producer.send(request);
        if (session.getTransacted())
        {
            session.commit();
        }

        Message response = consumer.receive(5000);
        if (session.getTransacted())
        {
            session.commit();
        }
        try
        {
            if (response instanceof MapMessage)
            {
                MapMessage bodyMap = (MapMessage) response;
                Map<String, Object> data = new HashMap<>();
                @SuppressWarnings("unchecked")
                Enumeration<String> keys = bodyMap.getMapNames();
                while (keys.hasMoreElements())
                {
                    String key = keys.nextElement();
                    data.put(key, bodyMap.getObject(key));
                }
                return data;
            }
            else if (response instanceof ObjectMessage)
            {
                Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> bodyMap = (Map<String, ?>) body;
                    return new HashMap<>(bodyMap);
                }
            }
            throw new AmqpManagementFacade.OperationUnsuccessfulException("Management read failed : "
                                                                          + response.getStringProperty("statusCode")
                                                                          + " - "
                                                                          + response.getStringProperty(
                    "statusDescription"),
                                                                          response.getIntProperty("statusCode"));
        }
        finally
        {
            consumer.close();
            if(_protocol == Protocol.AMQP_1_0)
            {
                ((TemporaryQueue)replyToDestination).delete();
            }
        }
    }

    public long getQueueDepth(final Queue destination, final Session session) throws Exception
    {
        final String escapedName = getEscapedName(destination);
        Map<String, Object> arguments = Collections.singletonMap("statistics",
                                                                 Collections.singletonList("queueDepthMessages"));
        Object statistics = performOperationUsingAmqpManagement(escapedName,
                                                                "getStatistics",
                                                                session,
                                                                "org.apache.qpid.Queue",
                                                                arguments);
        @SuppressWarnings("unchecked")
        Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
        return ((Number) statisticsMap.get("queueDepthMessages")).intValue();
    }

    public boolean isQueueExist(final Queue destination, final Session session) throws Exception
    {
        final String escapedName = getEscapedName(destination);
        try
        {
            performOperationUsingAmqpManagement(escapedName,
                                                "READ",
                                                session,
                                                "org.apache.qpid.Queue",
                                                Collections.emptyMap());
            return true;
        }
        catch (AmqpManagementFacade.OperationUnsuccessfulException e)
        {
            if (e.getStatusCode() == 404)
            {
                return false;
            }
            else
            {
                throw e;
            }
        }
    }

    private String getEscapedName(final Queue destination) throws JMSException
    {
        return destination.getQueueName().replaceAll("([/\\\\])", "\\\\$1");
    }

    private List<Map<String, Object>> getResultsAsMaps(final List<String> attributeNames,
                                                       final List<List<Object>> attributeValues)
    {
        List<Map<String, Object>> results = new ArrayList<>();
        for (List<Object> resultObject : attributeValues)
        {
            Map<String, Object> result = new HashMap<>();
            for (int i = 0; i < attributeNames.size(); ++i)
            {
                result.put(attributeNames.get(i), resultObject.get(i));
            }
            results.add(result);
        }
        return results;
    }

    public static class OperationUnsuccessfulException extends RuntimeException
    {
        private final int _statusCode;

        private OperationUnsuccessfulException(final String message, final int statusCode)
        {
            super(message == null ? String.format("Unexpected status code %d", statusCode): message);
            _statusCode = statusCode;
        }

        public int getStatusCode()
        {
            return _statusCode;
        }
    }
}
