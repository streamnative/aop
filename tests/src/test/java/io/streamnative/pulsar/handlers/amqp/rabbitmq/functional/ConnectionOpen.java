
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

package io.streamnative.pulsar.handlers.amqp.rabbitmq.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MalformedFrameException;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.impl.AMQCommand;
import com.rabbitmq.client.impl.SocketFrameHandler;
import com.rabbitmq.client.test.TestUtils;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeoutException;
import javax.net.SocketFactory;


/**
 * Check that protocol negotiation works.
 */
public class ConnectionOpen {
    //@Test
    public void correctProtocolHeader() throws IOException {
        SocketFrameHandler fh = new SocketFrameHandler(SocketFactory.getDefault().
                createSocket("localhost", AMQP.PROTOCOL.PORT));
        fh.sendHeader();
        AMQCommand command = new AMQCommand();
        while (!command.handleFrame(fh.readFrame())) {
        }
        Method m = command.getMethod();

        assertTrue("First command must be Connection.start",
                m instanceof AMQP.Connection.Start);
        AMQP.Connection.Start start = (AMQP.Connection.Start) m;
        assertTrue("Version in Connection.start is <= what we sent",
                start.getVersionMajor() < AMQP.PROTOCOL.MAJOR
                        || (start.getVersionMajor() == AMQP.PROTOCOL.MAJOR
                        && start.getVersionMinor() <= AMQP.PROTOCOL.MINOR));
    }

    //@Test
    public void crazyProtocolHeader() throws IOException {
        ConnectionFactory factory = TestUtils.connectionFactory();
        // keep the frame handler's socket
        Socket fhSocket = SocketFactory.getDefault().createSocket("localhost", AMQP.PROTOCOL.PORT);
        SocketFrameHandler fh = new SocketFrameHandler(fhSocket);
        fh.sendHeader(100, 3); // major, minor
        DataInputStream in = fh.getInputStream();
        // we should get a valid protocol header back
        byte[] header = new byte[4];
        in.read(header);
        // The protocol header is "AMQP" plus a version that the server
        // supports.  We can really only test for the first bit.
        assertEquals("AMQP", new String(header));
        in.read(header);
        assertEquals(in.available(), 0);
        // At this point the socket should have been closed.  We can
        // directly test for this, but since Socket.isClosed is purported to be
        // unreliable, we can also test whether trying to read more bytes
        // gives an error.
        if (!fhSocket.isClosed()) {
            fh.setTimeout(500);
            // NB the frame handler will return null if the socket times out
            try {
                fh.readFrame();
                fail("Expected socket read to fail due to socket being closed");
            } catch (MalformedFrameException mfe) {
                fail("Expected nothing, rather than a badly-formed something");
            } catch (IOException ioe) {
            }
        }
    }

    //@Test
    public void frameMaxLessThanFrameMinSize() throws IOException, TimeoutException {
        ConnectionFactory factory = TestUtils.connectionFactory();
        factory.setRequestedFrameMax(100);
        try {
            factory.newConnection();
        } catch (IOException ioe) {
            return;
        }
        fail("Broker should have closed the connection since our frame max < frame_min_size");
    }
}
