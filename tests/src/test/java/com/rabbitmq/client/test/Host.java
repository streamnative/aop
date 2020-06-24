

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

package com.rabbitmq.client.test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.NetworkConnection;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.streamnative.pulsar.handlers.amqp.ConnectionContainer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Host for rabbitmq client.
 */
public class Host {

    public static String capture(InputStream is)
            throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        StringBuilder buff = new StringBuilder();
        while ((line = br.readLine()) != null) {
            buff.append(line).append("\n");
        }
        return buff.toString();
    }

    public static Process executeCommand(String command) throws IOException {
        Process pr = executeCommandProcess(command);

        int ev = waitForExitValue(pr);
        if (ev != 0) {
            String stdout = capture(pr.getInputStream());
            String stderr = capture(pr.getErrorStream());
            throw new IOException("unexpected command exit value: "
                    + ev + "\ncommand: " + command + "\n"
                    + "\nstdout:\n" + stdout
                    + "\nstderr:\n" + stderr + "\n");
        }
        return pr;
    }

    private static int waitForExitValue(Process pr) {
        while (true) {
            try {
                pr.waitFor();
                break;
            } catch (InterruptedException ignored) {
            }
        }
        return pr.exitValue();
    }

    public static Process executeCommandIgnoringErrors(String command) throws IOException {
        Process pr = executeCommandProcess(command);
        waitForExitValue(pr);
        return pr;
    }

    private static Process executeCommandProcess(String command) throws IOException {
        String[] finalCommand;
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            finalCommand = new String[4];
            finalCommand[0] = "C:\\Windows\\System32\\cmd.exe";
            finalCommand[1] = "/y";
            finalCommand[2] = "/c";
            finalCommand[3] = command;
        } else {
            finalCommand = new String[3];
            finalCommand[0] = "/bin/sh";
            finalCommand[1] = "-c";
            finalCommand[2] = command;
        }
        return Runtime.getRuntime().exec(finalCommand);
    }

    public static boolean isRabbitMqCtlCommandAvailable(String command) throws IOException {
        Process process = rabbitmqctlIgnoreErrors("");
        String stderr = capture(process.getErrorStream());
        return stderr.contains(command);
    }

    public static Process rabbitmqctl(String command) throws IOException {
        return executeCommand(rabbitmqctlCommand()
                +
                " -n \'" + nodenameA() + "\'"

                +
                " " + command);
    }

    public static Process rabbitmqctlIgnoreErrors(String command) throws IOException {
        return executeCommandIgnoringErrors(rabbitmqctlCommand()
                +
                " -n \'" + nodenameA() + "\'"
                +
                " " + command);
    }

    public static void setResourceAlarm(String source) throws IOException {
        rabbitmqctl("eval 'rabbit_alarm:set_alarm({{resource_limit, " + source + ", node()}, []}).'");
    }

    public static void clearResourceAlarm(String source) throws IOException {
        rabbitmqctl("eval 'rabbit_alarm:clear_alarm({resource_limit, " + source + ", node()}).'");
    }

    public static Process invokeMakeTarget(String command) throws IOException {
        File rabbitmqctl = new File(rabbitmqctlCommand());
        return executeCommand(makeCommand()
                +
                " -C \'" + rabbitmqDir() + "\'"
                +
                " RABBITMQCTL=\'" + rabbitmqctl.getAbsolutePath() + "\'"
                +
                " RABBITMQ_NODENAME=\'" + nodenameA() + "\'"
                +
                " RABBITMQ_NODE_PORT=" + node_portA()
                +
                " RABBITMQ_CONFIG_FILE=\'" + config_fileA() + "\'"
                +
                " " + command);
    }

    public static void startRabbitOnNode() throws IOException {
        rabbitmqctl("start_app");
        tryConnectFor(10_000);
    }

    public static void stopRabbitOnNode() throws IOException {
        rabbitmqctl("stop_app");
    }

    public static void tryConnectFor(int timeoutInMs) throws IOException {
        tryConnectFor(timeoutInMs, node_portA() == null ? 5672 : Integer.valueOf(node_portA()));
    }

    public static void tryConnectFor(int timeoutInMs, int port) throws IOException {
        int waitTime = 100;
        int totalWaitTime = 0;
        while (totalWaitTime <= timeoutInMs) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            totalWaitTime += waitTime;
            ConnectionFactory connectionFactory = TestUtils.connectionFactory();
            connectionFactory.setPort(port);
            try (Connection ignored = connectionFactory.newConnection()) {
                return;

            } catch (Exception e) {
                // retrying
            }
        }
        throw new IOException("Could not connect to broker for " + timeoutInMs + " ms");
    }

    public static String makeCommand() {
        return System.getProperty("make.bin", "make");
    }

    public static String nodenameA() {
        return System.getProperty("test-broker.A.nodename");
    }

    public static String node_portA() {
        return System.getProperty("test-broker.A.node_port");
    }

    public static String config_fileA() {
        return System.getProperty("test-broker.A.config_file");
    }

    public static String nodenameB() {
        return System.getProperty("test-broker.B.nodename");
    }

    public static String node_portB() {
        return System.getProperty("test-broker.B.node_port");
    }

    public static String config_fileB() {
        return System.getProperty("test-broker.B.config_file");
    }

    public static String rabbitmqctlCommand() {
        return System.getProperty("rabbitmqctl.bin");
    }

    public static String rabbitmqDir() {
        return System.getProperty("rabbitmq.dir");
    }

    public static void closeConnection(AmqpConnection connection) throws IOException {
        ConnectionContainer.removeConnection(NamespaceName.get("public/vhost1"), connection);
        connection.getCtx().close();
    }

    public static void closeAllConnections() throws IOException {
        rabbitmqctl("close_all_connections 'Closed via rabbitmqctl'");
    }

    public static void closeConnection(NetworkConnection c) throws IOException {
        AmqpConnection ci = findConnectionInfoFor(
            ConnectionContainer.getNamespaceConnections(NamespaceName.get("public/vhost1")), c);
        closeConnection(ci);
    }

    /**
     * ConnectionInfo.
     */
    public static class ConnectionInfo {
        private final String pid;
        private final int peerPort;
        private final String clientProperties;

        public ConnectionInfo(String pid, int peerPort, String clientProperties) {
            this.pid = pid;
            this.peerPort = peerPort;
            this.clientProperties = clientProperties;
        }

        public String getPid() {
            return pid;
        }

        public int getPeerPort() {
            return peerPort;
        }

        public String getClientProperties() {
            return clientProperties;
        }

        @Override
        public String toString() {
            return "ConnectionInfo{"
                    +
                    "pid='" + pid + '\''
                    +
                    ", peerPort=" + peerPort
                    +
                    ", clientProperties='" + clientProperties + '\''
                    +
                    '}';
        }
    }

    public static List<ConnectionInfo> listConnections() throws IOException {
        String output = capture(rabbitmqctl("list_connections -q pid peer_port client_properties").getInputStream());
        String[] allLines = output.split("\n");

        ArrayList<ConnectionInfo> result = new ArrayList<ConnectionInfo>();
        for (String line : allLines) {
            if (line != null && !line.trim().isEmpty()) {
                String[] columns = line.split("\t");
                // can be also header line, so ignoring NumberFormatException
                try {
                    result.add(new ConnectionInfo(columns[0], Integer.valueOf(columns[1]), columns[2]));
                } catch (NumberFormatException e) {
                    // OK
                }
            }
        }
        return result;
    }

    private static AmqpConnection findConnectionInfoFor(Set<AmqpConnection> xs, NetworkConnection c) {
        AmqpConnection result = null;
        for (AmqpConnection ci : xs) {
            InetSocketAddress remoteAddress = (InetSocketAddress) ci.getCtx().channel().remoteAddress();
            if (c.getLocalPort() == remoteAddress.getPort()) {
                result = ci;
                break;
            }
        }
        return result;
    }
}
