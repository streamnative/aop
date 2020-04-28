

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

package io.streamnative.pulsar.handlers.amqp.functional;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.Host;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Base class for tests which would like a second, clustered node.
 */
public class ClusteredTestBase extends BrokerTestCase {
    // If these are non-null then the secondary node is up and clustered
    public Channel clusteredChannel;
    public Connection clusteredConnection;

    // These will always be non-null - if there is clustering they will point
    // to the secondary node, otherwise the primary
    public Channel alternateChannel;
    public Connection alternateConnection;

    @Override
    public void openChannel() throws IOException {
        super.openChannel();

        if (clusteredConnection != null) {
            clusteredChannel = clusteredConnection.createChannel();
        }

        alternateChannel = clusteredChannel == null ? channel : clusteredChannel;
    }

    private static boolean nonClusteredWarningPrinted;

    @Override
    public void openConnection() throws IOException, TimeoutException {
        super.openConnection();
        if (clusteredConnection == null) {
            try {
                ConnectionFactory cf2 = connectionFactory.clone();
                cf2.setHost("localhost");
                cf2.setPort(5673);
                clusteredConnection = cf2.newConnection();
            } catch (IOException e) {
                // Must be no secondary node
            }
        }

        if (clusteredConnection != null &&
                !clustered(connection, clusteredConnection)) {
            clusteredConnection.close();
            clusteredConnection = null;

            if (!nonClusteredWarningPrinted) {
                System.out.println("NOTE: Only one clustered node was detected - certain tests that");
                System.out.println("could test clustering will not do so.");
                nonClusteredWarningPrinted = true;
            }
        }

        alternateConnection = clusteredConnection == null ? connection : clusteredConnection;
    }

    private boolean clustered(Connection c1, Connection c2) throws IOException {
        Channel ch1 = c1.createChannel();
        Channel ch2 = c2.createChannel();
        // autodelete but not exclusive
        String q = ch1.queueDeclare("", false, false, true, null).getQueue();

        try {
            ch2.queueDeclarePassive(q);
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            // If we can't see the queue, secondary node must be up but not
            // clustered, hence not interesting to us
            return false;
        }

        ch1.queueDelete(q);
        ch1.abort();
        ch2.abort();

        return true;
    }

    @Override
    public void closeChannel() throws IOException {
        if (clusteredChannel != null) {
            clusteredChannel.abort();
            clusteredChannel = null;
            alternateChannel = null;
        }
        super.closeChannel();
    }

    @Override
    public void closeConnection() throws IOException {
        if (clusteredConnection != null) {
            clusteredConnection.abort();
            clusteredConnection = null;
            alternateConnection = null;
        }
        super.closeConnection();
    }

    protected void stopSecondary() throws IOException {
        Host.executeCommand(Host.rabbitmqctlCommand() +
                " -n '" + Host.nodenameB() + "'" +
                " stop_app");
    }

    protected void startSecondary() throws IOException {
        Host.executeCommand(Host.rabbitmqctlCommand() +
                " -n '" + Host.nodenameB() + "'" +
                " start_app");
        Host.tryConnectFor(10_000, Host.node_portB() == null ? 5673 : Integer.valueOf(Host.node_portB()));
    }
}
