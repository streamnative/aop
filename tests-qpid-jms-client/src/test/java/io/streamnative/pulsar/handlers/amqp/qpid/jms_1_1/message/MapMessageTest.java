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

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.junit.Test;

/**
 * MapMessageTest.
 */
public class MapMessageTest extends JmsTestBase
{
    private static final byte[] BYTES = { 99, 98, 97, 96, 95 };
    private static final String MESSAGE_ASCII = "Message";
    private static final String MESSAGE_NON_ASCII_UTF8 = "YEN\u00A5EURO\u20AC";
    private static final float SMALL_FLOAT = 100f;

    @Test
    public void sendAndReceiveEmpty() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            MapMessage message = session.createMapMessage();
            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("BytesMessage should be received", receivedMessage instanceof MapMessage);
            assertFalse("Unexpected map content", ((MapMessage) receivedMessage).getMapNames().hasMoreElements());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveBody() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            MapMessage message = session.createMapMessage();

            setMapValues(message);
            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("MapMessage should be received", receivedMessage instanceof MapMessage);
            assertTrue("Unexpected map content", ((MapMessage) receivedMessage).getMapNames().hasMoreElements());

            MapMessage receivedMapMessage = (MapMessage) receivedMessage;
            testMapValues(receivedMapMessage);
        }
        finally
        {
            connection.close();
        }
    }

    private void setMapValues(MapMessage message) throws JMSException
    {
        message.setBoolean("bool", true);
        message.setByte("byte",Byte.MAX_VALUE);
        message.setBytes("bytes", BYTES);
        message.setChar("char",'c');
        message.setDouble("double", Double.MAX_VALUE);
        message.setFloat("float", Float.MAX_VALUE);
        message.setFloat("smallfloat", SMALL_FLOAT);
        message.setInt("int",  Integer.MAX_VALUE);
        message.setLong("long",  Long.MAX_VALUE);
        message.setShort("short", Short.MAX_VALUE);
        message.setString("string-ascii", MESSAGE_ASCII);
        message.setString("string-utf8", MESSAGE_NON_ASCII_UTF8);

        // Test Setting Object Values
        message.setObject("object-bool", true);
        message.setObject("object-byte", Byte.MAX_VALUE);
        message.setObject("object-bytes", BYTES);
        message.setObject("object-char", 'c');
        message.setObject("object-double", Double.MAX_VALUE);
        message.setObject("object-float", Float.MAX_VALUE);
        message.setObject("object-int", Integer.MAX_VALUE);
        message.setObject("object-long", Long.MAX_VALUE);
        message.setObject("object-short", Short.MAX_VALUE);

        // Set a null String value
        message.setString("nullString", null);
        // Highlight protocol problem
        message.setString("emptyString", "");
    }

    private void testMapValues(MapMessage m) throws JMSException
    {
        // Test get<Primitive>

        // Boolean
        assertEquals(true, m.getBoolean("bool"));
        assertEquals(Boolean.TRUE.toString(), m.getString("bool"));

        // Byte
        assertEquals(Byte.MAX_VALUE, m.getByte("byte"));
        assertEquals(String.valueOf(Byte.MAX_VALUE), m.getString("byte"));

        // Bytes
        assertArrayEquals(BYTES, m.getBytes("bytes"));

        // Char
        assertEquals('c', m.getChar("char"));

        // Double
        assertEquals(Double.MAX_VALUE, m.getDouble("double"), 0);
        assertEquals("" + Double.MAX_VALUE, m.getString("double"));

        // Float
        assertEquals(Float.MAX_VALUE, m.getFloat("float"), 0);
        assertEquals(SMALL_FLOAT, (float) m.getDouble("smallfloat"), 0);
        assertEquals("" + Float.MAX_VALUE, m.getString("float"));

        // Integer
        assertEquals(Integer.MAX_VALUE, m.getInt("int"));
        assertEquals("" + Integer.MAX_VALUE, m.getString("int"));

        // long
        assertEquals(Long.MAX_VALUE, m.getLong("long"));
        assertEquals("" + Long.MAX_VALUE, m.getString("long"));

        // Short
        assertEquals(Short.MAX_VALUE, m.getShort("short"));
        assertEquals("" + Short.MAX_VALUE, m.getString("short"));
        assertEquals((int) Short.MAX_VALUE, m.getInt("short"));

        // String
        assertEquals(MESSAGE_ASCII, m.getString("string-ascii"));
        assertEquals(MESSAGE_NON_ASCII_UTF8, m.getString("string-utf8"));

        // Test getObjects
        assertEquals(true, m.getObject("object-bool"));
        assertEquals(Byte.MAX_VALUE, m.getObject("object-byte"));
        assertArrayEquals(BYTES, (byte[]) m.getObject("object-bytes"));
        assertEquals('c', m.getObject("object-char"));
        assertEquals(Double.MAX_VALUE, m.getObject("object-double"));
        assertEquals(Float.MAX_VALUE, m.getObject("object-float"));
        assertEquals(Integer.MAX_VALUE, m.getObject("object-int"));
        assertEquals(Long.MAX_VALUE, m.getObject("object-long"));
        assertEquals(Short.MAX_VALUE, m.getObject("object-short"));

        // Check Special values
        assertTrue(m.getString("nullString") == null);
        assertEquals("", m.getString("emptyString"));
    }
}
