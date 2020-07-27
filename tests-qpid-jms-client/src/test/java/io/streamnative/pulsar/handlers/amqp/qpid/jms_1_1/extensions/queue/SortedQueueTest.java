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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.queue.SortedQueue;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SortedQueueTest.
 */
@Ignore
public class SortedQueueTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SortedQueueTest.class);
    private static final String TEST_SORT_KEY = "testSortKey";
    private static final String[] VALUES = {" 73", " 18", " 11", "127", "166", "163", " 69", " 60", "191", "144",
            " 17", "161", "145", "140", "157", " 47", "136", " 56", "176", " 81",
            "195", " 96", "  2", " 68", "101", "141", "159", "187", "149", " 45",
            " 64", "100", " 83", " 51", " 79", " 82", "180", " 26", " 61", " 62",
            " 78", " 46", "147", " 91", "120", "164", " 92", "172", "188", " 50",
            "111", " 89", "  4", "  8", " 16", "151", "122", "178", " 33", "124",
            "171", "165", "116", "113", "155", "148", " 29", "  0", " 37", "131",
            "146", " 57", "112", " 97", " 23", "108", "123", "117", "167", " 52",
            " 98", "  6", "160", " 25", " 49", " 34", "182", "185", " 30", " 66",
            "152", " 58", " 86", "118", "189", " 84", " 36", "104", "  7", " 76",
            " 87", "  1", " 80", " 10", "142", " 59", "137", " 12", " 67", " 22",
            "  9", "106", " 75", "109", " 93", " 42", "177", "134", " 77", " 88",
            "114", " 43", "143", "135", " 55", "181", " 32", "174", "175", "184",
            "133", "107", " 28", "126", "103", " 85", " 38", "158", " 39", "162",
            "129", "194", " 15", " 24", " 19", " 35", "186", " 31", " 65", " 99",
            "192", " 74", "156", " 27", " 95", " 54", " 70", " 13", "110", " 41",
            " 90", "173", "125", "196", "130", "183", "102", "190", "132", "105",
            " 21", " 53", "139", " 94", "115", " 48", " 44", "179", "128", " 14",
            " 72", "119", "153", "168", "197", " 40", "150", "138", "  5", "154",
            "169", " 71", "199", "198", "170", "  3", "121", " 20", " 63", "193"};
    private static final String[] VALUES_SORTED =
            Arrays.stream(VALUES).sorted().collect(Collectors.toList()).toArray(new String[VALUES.length]);
    private static final int NUMBER_OF_MESSAGES = VALUES.length;
    private static final String[] SUBSET_KEYS = {"000", "100", "200", "300", "400", "500", "600", "700", "800", "900"};

    @Test
    public void testSortOrder() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createSortedQueue(queueName, TEST_SORT_KEY);
        sendMessages(queue);

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            for (String value : VALUES_SORTED)
            {
                Message received = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", received);
                assertEquals("Received message with unexpected sorted key value", value,
                             received.getStringProperty(TEST_SORT_KEY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testAutoAckSortedQueue() throws Exception
    {
        runThroughSortedQueueForSessionMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testTransactedSortedQueue() throws Exception
    {
        runThroughSortedQueueForSessionMode(Session.SESSION_TRANSACTED);
    }

    @Test
    public void testClientAckSortedQueue() throws Exception
    {
        runThroughSortedQueueForSessionMode(Session.CLIENT_ACKNOWLEDGE);
    }

    private void runThroughSortedQueueForSessionMode(final int sessionMode) throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createSortedQueue(queueName, TEST_SORT_KEY);
        final Connection consumerConnection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            final Session consumerSession = consumerConnection.createSession(sessionMode == Session.SESSION_TRANSACTED, sessionMode);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            final CountDownLatch receiveLatch = new CountDownLatch(NUMBER_OF_MESSAGES);
            CountingMessageListener listener = new CountingMessageListener(receiveLatch, consumerSession);
            consumer.setMessageListener(listener);
            consumerConnection.start();
            sendMessages(queue);
            assertTrue("Messages were not received during expected time",
                       receiveLatch.await(getReceiveTimeout() * NUMBER_OF_MESSAGES, TimeUnit.MILLISECONDS));
            assertNull("Unexpected exception in message listener", listener.getException());

            // make sure that all received messages are acknowledged before closing the session/connection
            // otherwise session close can timeout for auto-ack
            consumerSession.createTemporaryQueue().delete();
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testSortedQueueWithAscendingSortedKeys() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createSortedQueue(queueName, TEST_SORT_KEY);

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            final CountDownLatch receiveLatch = new CountDownLatch(NUMBER_OF_MESSAGES);
            consumer.setMessageListener(new CountingMessageListener(receiveLatch, consumerSession));
            consumerConnection.start();

            final Connection producerConnection = getConnection();
            try
            {
                final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
                final MessageProducer producer = producerSession.createProducer(queue);

                for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    final Message message = producerSession.createMessage();
                    message.setStringProperty(TEST_SORT_KEY, AscendingSortedKeys.getNextKey());
                    producer.send(message);
                    producerSession.commit();
                }
            }
            finally
            {
                producerConnection.close();
            }
            assertTrue("Messages were not received during expected time",
                       receiveLatch.await(getReceiveTimeout() * NUMBER_OF_MESSAGES, TimeUnit.MILLISECONDS));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testSortOrderWithNonUniqueKeys() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createSortedQueue(queueName, TEST_SORT_KEY);

        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            int count = 0;
            for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                final Message message = producerSession.createTextMessage("Message Text:" + count++);
                message.setStringProperty(TEST_SORT_KEY, "samesortkeyvalue");
                producer.send(message);
            }
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", received);
                assertEquals("Received message with unexpected sorted key value", "samesortkeyvalue",
                             received.getStringProperty(TEST_SORT_KEY));
                assertEquals("Received message with unexpected message value", "Message Text:" + i,
                             ((TextMessage) received).getText());
            }

            Message received = consumer.receive(getReceiveTimeout() / 4);
            assertNull("Unexpected message received", received);
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testSortOrderWithUniqueKeySubset() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createSortedQueue(queueName, TEST_SORT_KEY);

        int messageNumber = 100;
        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            int count = 0;
            while (count < messageNumber)
            {
                int keyValueIndex = count % SUBSET_KEYS.length;
                final Message msg = producerSession.createTextMessage("Message Text:" + count);
                msg.setStringProperty(TEST_SORT_KEY, SUBSET_KEYS[keyValueIndex]);
                producer.send(msg);
                count++;
            }
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            for (int i = 0; i < messageNumber; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", received);
                assertEquals("Received message with unexpected sorted key value", SUBSET_KEYS[i / 10],
                             received.getStringProperty(TEST_SORT_KEY));
            }

            Message received = consumer.receive(getReceiveTimeout() / 4);
            assertNull("Unexpected message received", received);
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testGetNextWithAck() throws Exception
    {
        final String queueName = getTestName();
        Queue queue = createSortedQueue(queueName, TEST_SORT_KEY);

        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            sendAndCommitMessage(producerSession, producer, "2");
            sendAndCommitMessage(producerSession, producer, "3");
            sendAndCommitMessage(producerSession, producer, "1");

            final Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
            try
            {
                final Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                final MessageConsumer consumer = consumerSession.createConsumer(queue);
                consumerConnection.start();

                Message received;

                //Receive 3 in sorted order
                received = assertReceiveAndValidateMessage(consumer, "1");
                received.acknowledge();
                received = assertReceiveAndValidateMessage(consumer, "2");
                received.acknowledge();
                received = assertReceiveAndValidateMessage(consumer, "3");
                received.acknowledge();

                //Send 1
                sendAndCommitMessage(producerSession, producer, "4");

                //Receive 1 and recover
                assertReceiveAndValidateMessage(consumer, "4");
                consumerSession.recover();

                //Receive same 1
                received = assertReceiveAndValidateMessage(consumer, "4");
                received.acknowledge();

                //Send 3 out of order
                sendAndCommitMessage(producerSession, producer, "7");
                sendAndCommitMessage(producerSession, producer, "6");
                sendAndCommitMessage(producerSession, producer, "5");

                //Receive 1 of 3 (possibly out of order due to pre-fetch)
                final Message messageBeforeRollback = assertReceiveMessage(consumer);
                consumerSession.recover();

                if (getProtocol().equals(Protocol.AMQP_0_10))
                {
                    //Receive 3 in sorted order (not as per JMS recover)
                    received = assertReceiveAndValidateMessage(consumer, "5");
                    received.acknowledge();
                    received = assertReceiveAndValidateMessage(consumer, "6");
                    received.acknowledge();
                    received = assertReceiveAndValidateMessage(consumer, "7");
                    received.acknowledge();
                }
                else
                {
                    //First message will be the one rolled-back (as per JMS spec).
                    final String messageKeyDeliveredBeforeRollback =
                            messageBeforeRollback.getStringProperty(TEST_SORT_KEY);
                    received = assertReceiveAndValidateMessage(consumer, messageKeyDeliveredBeforeRollback);
                    received.acknowledge();

                    //Remaining two messages will be sorted
                    final SortedSet<String> keys = new TreeSet<>(Arrays.asList("5", "6", "7"));
                    keys.remove(messageKeyDeliveredBeforeRollback);

                    received = assertReceiveAndValidateMessage(consumer, keys.first());
                    received.acknowledge();
                    received = assertReceiveAndValidateMessage(consumer, keys.last());
                    received.acknowledge();
                }
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    private Queue createSortedQueue(final String queueName, final String sortKey) throws Exception
    {
        createEntityUsingAmqpManagement(queueName,
                                        "org.apache.qpid.SortedQueue",
                                        Collections.singletonMap(SortedQueue.SORT_KEY, sortKey));
        return createQueue(queueName);
    }

    private void sendAndCommitMessage(final Session producerSession, final MessageProducer producer,
                                      final String keyValue) throws Exception
    {
        final Message message = producerSession.createTextMessage("Message Text: Key Value" + keyValue);
        message.setStringProperty(TEST_SORT_KEY, keyValue);
        producer.send(message);
        producerSession.commit();
    }

    private Message assertReceiveAndValidateMessage(final MessageConsumer consumer, final String expectedKey)
            throws Exception
    {
        final Message received = assertReceiveMessage(consumer);
        assertEquals("Received message with unexpected sorted key value", expectedKey,
                     received.getStringProperty(TEST_SORT_KEY));
        return received;
    }

    private Message assertReceiveMessage(final MessageConsumer consumer)
            throws JMSException
    {
        final Message received = consumer.receive(getReceiveTimeout());
        assertNotNull("Received message is unexpectedly null", received);
        return received;
    }

    private static class CountingMessageListener implements MessageListener
    {
        private final CountDownLatch _messagesReceiveLatch;
        private final Session _session;
        private final List<String> _receivedKeys;
        private int _count = 0;
        private volatile Exception _exception;

        CountingMessageListener(final CountDownLatch messagesReceiveLatch, final Session session)
        {
            _messagesReceiveLatch = messagesReceiveLatch;
            _session = session;
            _receivedKeys = new CopyOnWriteArrayList<>();
        }

        @Override
        public void onMessage(final Message message)
        {
            try
            {
                if (_session.getTransacted())
                {
                    if (_count % 10 == 0)
                    {
                        LOGGER.debug("transacted session rollback");
                        _session.rollback();
                    }
                    else
                    {
                        LOGGER.debug("transacted session commit");
                        _session.commit();
                        _receivedKeys.add(message.getStringProperty(TEST_SORT_KEY));
                        _messagesReceiveLatch.countDown();
                    }
                }
                else if (_session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
                {
                    if (_count % 10 == 0)
                    {
                        LOGGER.debug("client ack session recover");
                        _session.recover();
                    }
                    else
                    {
                        LOGGER.debug("client ack session acknowledge");
                        message.acknowledge();
                        _receivedKeys.add(message.getStringProperty(TEST_SORT_KEY));
                        _messagesReceiveLatch.countDown();
                    }
                }
                else
                {
                    LOGGER.debug("auto ack session");
                    _receivedKeys.add(message.getStringProperty(TEST_SORT_KEY));
                    _messagesReceiveLatch.countDown();
                }

                _count++;
                LOGGER.debug("Message consumed with key: " + message.getStringProperty(TEST_SORT_KEY));
                LOGGER.debug("Message consumed with consumed index: " + _receivedKeys.size());
            }
            catch (Exception e)
            {
                LOGGER.error("Exception in listener", e);
                _exception = e;
            }
        }

        public Exception getException()
        {
            return _exception;
        }
    }

    private static class AscendingSortedKeys
    {
        static final String[] KEYS = {"Ul4a1", "WaWsv", "2Yz7E", "ix74r", "okgRi", "HlUbF", "LewvM", "lweGy",
                "TXQ0Z", "0Kyfs", "s7Mxk", "dmoS7", "8RCUA", "W3VFH", "aez9y", "uQIcz", "0h1b1", "cmXIX",
                "4dEz6", "zHF1q", "D6rBy", "5drc6", "0BmCy", "BCxeC", "t59lR", "aL6AJ", "OHaBz", "WmadA",
                "B3qem", "CxVEf", "AIYUu", "uJScX", "uoStw", "ogLgc", "AgJHQ", "hUTw7", "Rxrsm", "9GXkX",
                "7hyVv", "y94nw", "Twano", "TCgPp", "pFrrl", "POUYS", "L7cGc", "0ao3l", "CNHmv", "MaJQs",
                "OUqFM", "jeskS", "FPfSE", "v1Hln", "14FLR", "KZamH", "G1RhS", "FVMxo", "rKDLJ", "nnP8o",
                "nFqik", "zmLYD", "1j5L8", "e6e4z", "WDVWJ", "aDGtS", "fcwDa", "nlaBy", "JJs5m", "vLsmS",
                "No0Qb", "JXljW", "Waim6", "MezSW", "l83Ud", "SjskQ", "uPX7G", "5nmWv", "ZhwG1", "uTacx",
                "t98iW", "JkzUn", "fmIK1", "i7WMQ", "bgJAz", "n1pmO", "jS1aj", "4W0Tl", "Yf2Ec", "sqVrf",
                "MojnP", "qQxHP", "pWiOs", "yToGW", "kB5nP", "BpYhV", "Cfgr3", "zbIYY", "VLTy6", "he9IA",
                "lm0pD", "WreyP", "8hJdt", "QnJ1S", "n8pJ9", "iqv4k", "OUYuF", "8cVD3", "sx5Gl", "cQOnv",
                "wiHrZ", "oGu6x", "7fsYM", "gf8rI", "7fKYU", "pT8wu", "lCMxy", "prNT6", "5Drn0", "guMb8",
                "OxWIH", "uZPqg", "SbRYy", "In3NS", "uvf7A", "FLsph", "pmeCd", "BbwgA", "ru4UG", "YOfrY",
                "W7cTs", "K4GS8", "AOgEe", "618Di", "dpe1v", "3otm6", "oVQp6", "5Mg9r", "Y1mC0", "VIlwP",
                "aFFss", "Mkgy8", "pv0i7", "S77LH", "XyPZN", "QYxC0", "vkCHH", "MGlTF", "24ARF", "v2eC3",
                "ZUnqt", "HfyNQ", "FjHXR", "45cIH", "1LB1L", "zqH0W", "fLNg8", "oQ87r", "Cp3mZ", "Zv7z0",
                "O3iyQ", "EOE1o", "5ZaEz", "tlILt", "MmsIo", "lXFOB", "gtCA5", "yEfy9", "7X3uy", "d7vjM",
                "XflUq", "Fhtgl", "NOHsz", "GWqqX", "xciqp", "BFkb8", "P6bcg", "lViBv", "2TRI7", "2hEEU",
                "9XyT9", "29QAz", "U3yw5", "FxX9q", "C2Irc", "8U2nU", "m4bxU", "5iGN5", "mX2GE", "cShY2",
                "JRJQB", "yvOMI", "4QMc9", "NAFuw", "RmDcr", "faHir", "2ZHdk", "zY1GY", "a00b5", "ZuDtD",
                "JIqXi", "K20wK", "gdQsS", "5Namm", "lkMUA", "IBe8k", "FcWrW", "FFDui", "tuDyS", "ZJTXH",
                "AkKTk", "zQt6Q", "FNYIM", "RpBQm", "RsQUq", "Mm8si", "gjUTu", "zz4ZU", "jiVBP", "ReKEW",
                "5VZjS", "YjB9t", "zFgtB", "8TxD7", "euZA5", "MK07Y", "CK5W7", "16lHc", "6q6L9", "Z4I1v",
                "UlU3M", "SWfou", "0PktI", "55rfB", "jfREu", "580YD", "Uvlv4", "KASQ8", "AmdQd", "piJSk",
                "hE1Ql", "LDk6f", "NcICA", "IKxdL", "iwzGk", "uN6r3", "lsQGo", "QClRL", "iKqhr", "FGzgp",
                "RkQke", "b29RJ", "CIShG", "9eoRc", "F6PT2", "LbRTH", "M3zXL", "GXdoH", "IjTwP", "RBhp0",
                "yluBx", "mz8gx", "MmKGJ", "Q6Lix", "uupzk", "RACuj", "d85a9", "qaofN", "kZANm", "jtn0X",
                "lpF6W", "suY4x", "rz7Ut", "wDajX", "1v5hH", "Yw2oU", "ksJby", "WMiS3", "lj07Q", "EdBKc",
                "6AFT0", "0YAGH", "ThjNn", "JKWYR", "9iGoT", "UmaEv", "3weIF", "CdyBV", "pAhR1", "djsrv",
                "xReec", "8FmFH", "Dz1R3", "Ta8f6", "DG4sT", "VjCZq", "jSjS3", "Pb1pa", "VNCPd", "Kr8ys",
                "AXpwE", "ZzJHW", "Nxx9V", "jzUqR", "dhSuH", "DQimp", "07n1c", "HP433", "RzaZA", "cL0aE",
                "Ss0Zu", "FnPFB", "7lUXZ", "9rlg9", "lH1kt", "ni2v1", "48cHL", "soy9t", "WPmlx", "2Nslm",
                "hSSvQ", "9y4lw", "ulk41", "ECMvU", "DLhzM", "GrDg7", "x3LDe", "QChxs", "xXTI4", "Gv3Fq",
                "rhl0J", "QssNC", "brhlQ", "s93Ml", "tl72W", "pvgjS", "Qworu", "DcpWB", "X6Pin", "J2mQi",
                "BGaQY", "CqqaD", "NhXdu", "dQ586", "Yh1hF", "HRxd8", "PYBf4", "64s8N", "tvdkD", "azIWp",
                "tAOsr", "v8yFN", "h1zcH", "SmGzv", "bZLvS", "fFDrJ", "Oz8yZ", "0Wr5y", "fcJOy", "7ku1p",
                "QbxXc", "VerEA", "QWxoT", "hYBCK", "o8Uyd", "FwEJz", "hi5X7", "uAWyp", "I7p2a", "M6qcG",
                "gIYvE", "HzZT8", "iB08l", "StlDJ", "tjQxs", "k85Ae", "taOXK", "s4786", "2DREs", "atef2",
                "Vprf2", "VBjhz", "EoToP", "blLA9", "qUJMd", "ydG8U", "8xEKz", "uLtKs", "GSQwj", "S2Dfu",
                "ciuWz", "i3pyd", "7Ow5C", "IRh48", "vOqCE", "Q6hMC", "yofH3", "KsjRK", "5IhmG", "fqypy",
                "0MR5X", "Chuy3"};

        private static int _i = 0;
        private static int _j = 0;

        static
        {
            Arrays.sort(KEYS);
        }

        static String getNextKey()
        {
            if (_j == KEYS.length)
            {
                _j = 0;
                _i++;
                if (_i == KEYS.length)
                {
                    _i = 0;
                }
            }
            return KEYS[_i] + "-" + KEYS[_j++];
        }
    }

    private void sendMessages(final Queue queue) throws JMSException, NamingException
    {
        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            for (String value : VALUES)
            {
                final Message msg = producerSession.createMessage();
                msg.setStringProperty(TEST_SORT_KEY, value);
                producer.send(msg);
            }

            producerSession.commit();
            producer.close();
        }
        finally
        {
            producerConnection.close();
        }
    }
}
