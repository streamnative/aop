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
package io.streamnative.pulsar.handlers.amqp.qpid;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * Qpid-JMS client test.
 */
@Slf4j
public class QpidJMSClientTest extends QpidTestBase {

    @Test
    public void basicTest() throws Exception {
        System.out.println("In runTest");
        System.setProperty("qpid.amqp.version", "0-9-1");
        Properties properties = new Properties();

        properties.put("java.naming.factory.initial", "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        properties.put("connectionfactory.qpidConnectionFactory",
                "amqp://guest:guest@clientid/vhost1?brokerlist='tcp://127.0.0.1:"
                        + getAmqpBrokerPortList().get(0) + "'");
        properties.put("queue.myqueue", "queue1");

        System.out.println("properties loaded");
        Context context = new InitialContext(properties);

        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionFactory");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        System.out.println("Connection started");

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = (Queue) context.lookup("myqueue");
        System.out.println("Session created");

        MessageConsumer messageConsumer = session.createConsumer(queue);
        MessageProducer messageProducer = session.createProducer(queue);

        int messageCnt = 1000;
        for (int i = 0; i < messageCnt; i++) {
            TextMessage message = session.createTextMessage("Hello world!");
            messageProducer.send(message);
            session.commit();
        }

        AtomicInteger receiveMsgCnt = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(messageCnt);
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                countDownLatch.countDown();
                receiveMsgCnt.incrementAndGet();
                try {
                    System.out.println(((TextMessage) message).getText());
                    session.commit();
                } catch (Exception e) {
                    log.error("Consume messages error.", e);
                    Assert.fail("Consume messages error.");
                }
            }
        });
        countDownLatch.await();
        Assert.assertEquals(messageCnt, receiveMsgCnt.get());

        connection.close();
        context.close();
    }

}
