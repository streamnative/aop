/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Ignore;
import org.junit.Test;

/**
 * ObjectMessageTest.
 */
public class ObjectMessageTest extends JmsTestBase
{
    @Test
    public void sendAndReceive() throws Exception
    {
        UUID test = UUID.randomUUID();
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage(test);
            Object o = testMessage.getObject();

            assertNotNull("Object was null", o);
            assertNotNull("toString returned null", testMessage.toString());

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals("First read: UUIDs were not equal", test, result);

            result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals("Second read: UUIDs were not equal", test, result);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveNull() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage(null);
            Object o = testMessage.getObject();

            assertNull("Object was not null", o);
            assertNotNull("toString returned null", testMessage.toString());

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals("First read: UUIDs were not equal", null, result);

            result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals("Second read: UUIDs were not equal", null, result);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendEmptyObjectMessage() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage();

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals("First read: unexpected object received", null, result);

            result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals("Second read: unexpected object received", null, result);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveObject() throws Exception
    {
        A a1 = new A(1, "A");
        A a2 = new A(2, "a");
        B b = new B(1, "B");
        C<String, Object> c = new C<>();
        c.put("A1", a1);
        c.put("a2", a2);
        c.put("B", b);
        c.put("String", "String");
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage(c);

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertTrue("Unexpected object received", result instanceof C);

            @SuppressWarnings("unchecked") final C<String, Object> received = (C) result;
            assertEquals("Unexpected size", c.size(), received.size());
            assertEquals("Unexpected keys", new HashSet<>(c.keySet()), new HashSet<>(received.keySet()));

            for (String key : c.keySet())
            {
                assertEquals("Unexpected value for " + key, c.get(key), received.get(key));
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForString() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        String testStringProperty = "TestStringProperty";
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestStringProperty", testStringProperty);
            assertEquals(testStringProperty, msg.getObjectProperty("TestStringProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         testStringProperty,
                         receivedMessage.getObjectProperty("TestStringProperty"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testSetObjectPropertyForBoolean() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestBooleanProperty", Boolean.TRUE);
            assertEquals(Boolean.TRUE, msg.getObjectProperty("TestBooleanProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         Boolean.TRUE,
                         receivedMessage.getObjectProperty("TestBooleanProperty"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testSetObjectPropertyForByte() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestByteProperty", Byte.MAX_VALUE);
            assertEquals(Byte.MAX_VALUE, msg.getObjectProperty("TestByteProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         Byte.MAX_VALUE,
                         receivedMessage.getObjectProperty("TestByteProperty"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testSetObjectPropertyForShort() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestShortProperty", Short.MAX_VALUE);
            assertEquals(Short.MAX_VALUE, msg.getObjectProperty("TestShortProperty"));
            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         Short.MAX_VALUE,
                         receivedMessage.getObjectProperty("TestShortProperty"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testSetObjectPropertyForInteger() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestIntegerProperty", Integer.MAX_VALUE);
            assertEquals(Integer.MAX_VALUE, msg.getObjectProperty("TestIntegerProperty"));
            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         Integer.MAX_VALUE,
                         receivedMessage.getObjectProperty("TestIntegerProperty"));
        }
        finally
        {
            connection.close();
        }
    }


    @Test
    @Ignore
    public void testSetObjectPropertyForDouble() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestDoubleProperty", Double.MAX_VALUE);
            assertEquals(Double.MAX_VALUE, msg.getObjectProperty("TestDoubleProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         Double.MAX_VALUE,
                         receivedMessage.getObjectProperty("TestDoubleProperty"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testSetObjectPropertyForFloat() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestFloatProperty", Float.MAX_VALUE);
            assertEquals(Float.MAX_VALUE, msg.getObjectProperty("TestFloatProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            assertEquals("Unexpected received property",
                         Float.MAX_VALUE,
                         receivedMessage.getObjectProperty("TestFloatProperty"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClearBodyAndProperties() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            A object = new A(1, "test");
            ObjectMessage msg = session.createObjectMessage(object);
            msg.setStringProperty("testProperty", "testValue");

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("ObjectMessage should be received", receivedMessage instanceof ObjectMessage);
            ObjectMessage objectMessage = (ObjectMessage) receivedMessage;
            Object received = objectMessage.getObject();
            assertTrue("Unexpected object type received", received instanceof A);
            assertEquals("Unexpected object received", object, received);
            assertEquals("Unexpected property value", "testValue", receivedMessage.getStringProperty("testProperty"));

            try
            {
                objectMessage.setObject("Test text");
                fail("Message should not be writable");
            }
            catch (MessageNotWriteableException e)
            {
                // pass
            }

            objectMessage.clearBody();

            try
            {
                objectMessage.setObject("Test text");
            }
            catch (MessageNotWriteableException e)
            {
                fail("Message should be writable");
            }

            try
            {
                objectMessage.setStringProperty("test", "test");
                fail("Message should not be writable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // pass
            }

            objectMessage.clearProperties();

            try
            {
                objectMessage.setStringProperty("test", "test");
            }
            catch (MessageNotWriteableException mnwe)
            {
                fail("Message should be writable");
            }
        }
        finally
        {
            connection.close();
        }
    }

    private static class A implements Serializable
    {
        private String sValue;
        private int iValue;

        A(int i, String s)
        {
            sValue = s;
            iValue = i;
        }

        @Override
        public int hashCode()
        {
            return iValue;
        }

        @Override
        public boolean equals(Object o)
        {
            return (o instanceof A) && equals((A) o);
        }

        protected boolean equals(A a)
        {
            return ((a.sValue == null) ? (sValue == null) : a.sValue.equals(sValue)) && (a.iValue == iValue);
        }
    }

    private static class B extends A
    {
        private long time;

        B(int i, String s)
        {
            super(i, s);
            time = System.currentTimeMillis();
        }

        @Override
        protected boolean equals(A a)
        {
            return super.equals(a) && (a instanceof B) && (time == ((B) a).time);
        }
    }

    private static class C<X, Y> extends HashMap<X, Y> implements Serializable
    {
    }
}
