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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.tls;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.AmqpManagementFacade;
import io.streamnative.pulsar.handlers.amqp.qpid.core.BrokerAdmin;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.BrokerManagementHelper;
import io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.TlsHelper;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * TlsTest.
 */
public class TlsTest extends JmsTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static TlsHelper _tlsHelper;

    @BeforeClass
    public static void setUp() throws Exception
    {
        _tlsHelper = new TlsHelper(TLS_RESOURCE);

        System.setProperty("javax.net.debug", "ssl");

        // workaround for QPID-8069
        if (getProtocol() != Protocol.AMQP_1_0 && getProtocol() != Protocol.AMQP_0_10)
        {
            System.setProperty("amqj.MaximumStateWait", "4000");
        }

    }

    @AfterClass
    public static void tearDown()
    {
        System.clearProperty("javax.net.debug");
        if (getProtocol() != Protocol.AMQP_1_0)
        {
            System.clearProperty("amqj.MaximumStateWait");
        }
    }

    @Test
    public void testCreateSSLConnectionUsingConnectionURLParams() throws Exception
    {
        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setKeyStoreLocation(_tlsHelper.getClientKeyStore())
                                                      .setKeyStorePassword(TLS_RESOURCE.getSecret())
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCreateSSLConnectionWithCertificateTrust() throws Exception
    {
        assumeThat("Qpid JMS Client does not support trusting of a certificate",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        int port = configureTlsPort(getTestPortName(), false, false, false);
        File trustCertFile = TLS_RESOURCE.saveCertificateAsPem(_tlsHelper.getCaCertificate()).toFile();

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setOptions(Collections.singletonMap("trusted_certs_path",
                                                                                           encodePathOption(trustCertFile.getCanonicalPath())))
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSSLConnectionToPlainPortRejected() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        setSslStoreSystemProperties();
        try
        {
            InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
            getConnectionBuilder().setPort(brokerAddress.getPort())
                                  .setHost(brokerAddress.getHostName())
                                  .setTls(true)
                                  .build();

            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testHostVerificationIsOnByDefault() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setHost("127.0.0.1")
                                  .setTls(true)
                                  .setKeyStoreLocation(_tlsHelper.getClientKeyStore())
                                  .setKeyStorePassword(TLS_RESOURCE.getSecret())
                                  .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                  .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                  .build();
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
        }

        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost("127.0.0.1")
                                                      .setTls(true)
                                                      .setKeyStoreLocation(_tlsHelper.getClientKeyStore())
                                                      .setKeyStorePassword(TLS_RESOURCE.getSecret())
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .setVerifyHostName(false)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCreateSslConnectionUsingJVMSettings() throws Exception
    {
        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);
        setSslStoreSystemProperties();
        try
        {
            Connection connection = getConnectionBuilder().setPort(port)
                                                          .setTls(true)
                                                          .build();
            try
            {
                assertConnection(connection);
            }
            finally
            {
                connection.close();
            }
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testMultipleCertsInSingleStore() throws Exception
    {
        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);
        setSslStoreSystemProperties();
        try
        {
            Connection connection = getConnectionBuilder().setClientId(getTestName())
                                                          .setPort(port)
                                                          .setTls(true)
                                                          .setKeyAlias(TlsHelper.CERT_ALIAS_APP1)
                                                          .build();
            try
            {
                assertConnection(connection);
            }
            finally
            {
                connection.close();
            }

            Connection connection2 = getConnectionBuilder().setPort(port)
                                                           .setTls(true)
                                                           .setKeyAlias(TlsHelper.CERT_ALIAS_APP2)
                                                           .build();
            try
            {
                assertConnection(connection2);
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testVerifyHostNameWithIncorrectHostname() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        setSslStoreSystemProperties();
        try
        {
            getConnectionBuilder().setPort(port)
                                  .setHost("127.0.0.1")
                                  .setTls(true)
                                  .setVerifyHostName(true)
                                  .build();
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testVerifyLocalHost() throws Exception
    {
        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        setSslStoreSystemProperties();
        try
        {
            Connection connection = getConnectionBuilder().setPort(port)
                                                          .setHost("localhost")
                                                          .setTls(true)
                                                          .build();
            try
            {
                assertConnection(connection);
            }
            finally
            {
                connection.close();
            }
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testCreateSSLConnectionUsingConnectionURLParamsTrustStoreOnly() throws Exception
    {
        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClientCertificateMissingWhilstNeeding() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setHost(getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getHostName())
                                  .setTls(true)
                                  .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                  .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                  .build();
            fail("Connection was established successfully");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    @Test
    public void testClientCertificateMissingWhilstWanting() throws Exception
    {
        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClientCertMissingWhilstWantingAndNeeding() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));
        //Start the broker (NEEDing and WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, true, false);

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setHost(getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getHostName())
                                  .setTls(true)
                                  .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                  .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                  .build();
            fail("Connection was established successfully");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    @Test
    public void testCreateSSLandTCPonSamePort() throws Exception
    {

        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, true);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setKeyStoreLocation(_tlsHelper.getClientKeyStore())
                                                      .setKeyStorePassword(TLS_RESOURCE.getSecret())
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnectionBuilder().setPort(port)
                                                       .setHost(brokerAddress.getHostName())
                                                       .build();
        try
        {
            assertConnection(connection2);
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void testCreateSSLWithCertFileAndPrivateKey() throws Exception
    {
        assumeThat("Qpid JMS Client does not support trusting of a certificate",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        assumeThat("QPID-8255: certificate can only be loaded using jks keystore ",
                   java.security.KeyStore.getDefaultType(),
                   is(equalTo("jks")));

        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        clearSslStoreSystemProperties();

        final Map<String, String> options = new HashMap<>();
        File keyFile = TLS_RESOURCE.savePrivateKeyAsPem(_tlsHelper.getClientPrivateKey()).toFile();
        File certificateFile = TLS_RESOURCE.saveCertificateAsPem(_tlsHelper.getClientCerificate(), _tlsHelper.getCaCertificate()).toFile();
        options.put("client_cert_path", encodePathOption(certificateFile.getCanonicalPath()));
        options.put("client_cert_priv_key_path", encodePathOption(keyFile.getCanonicalPath()));
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .setVerifyHostName(false)
                                                      .setOptions(options)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }


    private int configureTlsPort(final String portName,
                                 final boolean needClientAuth,
                                 final boolean wantClientAuth,
                                 final boolean samePort) throws Exception
    {

        final String keyStoreName = portName + "KeyStore";
        final String trustStoreName = portName + "TrustStore";
        try (final BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                              new AmqpManagementFacade(getProtocol())))
        {
            helper.openManagementConnection();

            final String authenticationManager = helper.getAuthenticationProviderNameForAmqpPort(getBrokerAdmin().getBrokerAddress(
                    BrokerAdmin.PortType.AMQP).getPort());
            return helper.createKeyStore(keyStoreName, _tlsHelper.getBrokerKeyStore(), TLS_RESOURCE.getSecret())
                         .createTrustStore(trustStoreName, _tlsHelper.getBrokerTrustStore(), TLS_RESOURCE.getSecret())
                         .createAmqpTlsPort(portName,
                                            authenticationManager,
                                            keyStoreName,
                                            samePort,
                                            needClientAuth,
                                            wantClientAuth,
                                            trustStoreName).getAmqpBoundPort(portName);
        }
    }

    private void setSslStoreSystemProperties()
    {
        System.setProperty("javax.net.ssl.keyStore", _tlsHelper.getClientKeyStore());
        System.setProperty("javax.net.ssl.keyStorePassword", TLS_RESOURCE.getSecret());
        System.setProperty("javax.net.ssl.trustStore", _tlsHelper.getClientTrustStore());
        System.setProperty("javax.net.ssl.trustStorePassword", TLS_RESOURCE.getSecret());
    }

    private void clearSslStoreSystemProperties()
    {
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }

    private String getTestPortName()
    {
        return getTestName() + "TlsPort";
    }

    private void assertConnection(final Connection connection) throws JMSException
    {
        assertNotNull("connection should be successful", connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull("create session should be successful", session);
    }

    private String encodePathOption(final String canonicalPath)
    {
        try
        {
            return URLEncoder.encode(canonicalPath, StandardCharsets.UTF_8.name()).replace("+", "%20");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
