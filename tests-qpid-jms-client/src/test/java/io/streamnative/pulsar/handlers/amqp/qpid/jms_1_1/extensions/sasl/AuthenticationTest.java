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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.sasl;

import static org.apache.qpid.test.utils.UnitTestBase.getJvmVendor;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.AmqpManagementFacade;
import io.streamnative.pulsar.handlers.amqp.qpid.core.ConnectionBuilder;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.BrokerManagementHelper;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HashedNegotiator;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.JvmVendor;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * AuthenticationTest.
 */
public class AuthenticationTest extends JmsTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final String DN_CA = "CN=MyRootCA,O=ACME,ST=Ontario,C=CA";
    private static final String DN_BROKER = "CN=localhost,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown";
    private static final String DN_INTERMEDIATE = "CN=intermediate_ca@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_APP1 = "CN=app1@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_APP2 = "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_INT =
            "CN=allowed_by_ca_with_intermediate@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_ALLOWED = "CN=allowed_by_ca@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_REVOKED = "CN=revoked_by_ca@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_REVOKED_BY_EMPTY =
            "CN=revoked_by_ca_empty_crl@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_REVOKED_INVALID_CRL =
            "CN=revoked_by_ca_invalid_crl_path@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_UNTRUSTED = "CN=untrusted_client";
    private static final String CERT_ALIAS_ROOT_CA = "rootca";
    private static final String CERT_ALIAS_APP1 = "app1";
    private static final String CERT_ALIAS_APP2 = "app2";
    private static final String CERT_ALIAS_ALLOWED = "allowed_by_ca";
    private static final String CERT_ALIAS_REVOKED = "revoked_by_ca";
    private static final String CERT_ALIAS_REVOKED_EMPTY_CRL = "revoked_by_ca_empty_crl";
    private static final String CERT_ALIAS_REVOKED_INVALID_CRL_PATH = "revoked_by_ca_invalid_crl_path";
    private static final String CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE = "allowed_by_ca_with_intermediate";
    private static final String CERT_ALIAS_UNTRUSTED_CLIENT = "untrusted_client";

    private static final String USER = "user";
    private static final String USER_PASSWORD = "user";

    private static final Server CRL_SERVER = new Server();
    private static final HandlerCollection HANDLERS = new HandlerCollection();

    private static final String CRL_TEMPLATE = "http://localhost:%d/%s";

    private static int crlHttpPort = -1;
    private static String _brokerKeyStore;
    private static String _brokerTrustStore;
    private static String _clientKeyStore;
    private static String _clientTrustStore;
    private static String _brokerPeerStore;
    private static String _clientExpiredKeyStore;
    private static String _clientUntrustedKeyStore;
    private static Path _crlFile;
    private static Path _emptyCrlFile;
    private static Path _intermediateCrlFile;

    @BeforeClass
    public static void setUp() throws Exception
    {
        _crlFile = TLS_RESOURCE.createFile(".crl");
        _emptyCrlFile = TLS_RESOURCE.createFile("-empty.crl");
        _intermediateCrlFile = TLS_RESOURCE.createFile("-intermediate.crl");

        // workaround for QPID-8069
        if (getProtocol() != Protocol.AMQP_1_0 && getProtocol() != Protocol.AMQP_0_10)
        {
            System.setProperty("amqj.MaximumStateWait", "4000");
        }

        final ServerConnector connector = new ServerConnector(CRL_SERVER);
        connector.setPort(0);
        connector.setHost("localhost");

        CRL_SERVER.addConnector(connector);
        createContext(_crlFile);
        createContext(_emptyCrlFile);
        createContext(_intermediateCrlFile);
        CRL_SERVER.setHandler(HANDLERS);
        CRL_SERVER.start();
        crlHttpPort = connector.getLocalPort();

        buildTlsResources();

        System.setProperty("javax.net.debug", "ssl");
    }

    private static void buildTlsResources() throws Exception
    {
        final String crlUri = String.format(CRL_TEMPLATE, crlHttpPort, _crlFile.toFile().getName());
        final String emptyCrlUri = String.format(CRL_TEMPLATE, crlHttpPort, _emptyCrlFile.toFile().getName());
        final String intermediateCrlUri = String.format(CRL_TEMPLATE, crlHttpPort, _intermediateCrlFile.toFile().getName());
        final String nonExistingCrlUri = String.format(CRL_TEMPLATE, crlHttpPort, "not/a/crl");

        final KeyCertificatePair caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final KeyPair brokerKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate brokerCertificate =
                TlsResourceBuilder.createCertificateForServerAuthorization(brokerKeyPair, caPair, DN_BROKER);

        _brokerKeyStore = TLS_RESOURCE.createKeyStore(new PrivateKeyEntry("java-broker",
                                                                          brokerKeyPair.getPrivate(),
                                                                          brokerCertificate,
                                                                          caPair.getCertificate()),
                                                      new CertificateEntry(CERT_ALIAS_ROOT_CA,
                                                                           caPair.getCertificate()))
                                      .toFile()
                                      .getAbsolutePath();
        _brokerTrustStore = TLS_RESOURCE.createKeyStore(new CertificateEntry(CERT_ALIAS_ROOT_CA,
                                                                             caPair.getCertificate()))
                                        .toFile()
                                        .getAbsolutePath();

        final KeyPair clientApp1KeyPair = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientApp1Certificate =
                TlsResourceBuilder.createCertificateForClientAuthorization(clientApp1KeyPair,
                                                                           caPair, DN_CLIENT_APP1);

        _brokerPeerStore = TLS_RESOURCE.createKeyStore(new CertificateEntry(DN_CLIENT_APP1,
                                                                            clientApp1Certificate))
                                       .toFile()
                                       .getAbsolutePath();

        final KeyPair clientApp2KeyPair = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientApp2Certificate =
                TlsResourceBuilder.createCertificateForClientAuthorization(clientApp2KeyPair,
                                                                           caPair, DN_CLIENT_APP2);

        final KeyPair clientAllowedKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientAllowedCertificate =
                TlsResourceBuilder.createCertificateWithCrlDistributionPoint(clientAllowedKeyPair,
                                                                             caPair,
                                                                             DN_CLIENT_ALLOWED,
                                                                             crlUri);

        final KeyPair clientRevokedKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientRevokedCertificate =
                TlsResourceBuilder.createCertificateWithCrlDistributionPoint(clientRevokedKeyPair,
                                                                             caPair,
                                                                             DN_CLIENT_REVOKED,
                                                                             crlUri);

        final KeyPair clientKeyPairRevokedByEmpty = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientCertificateRevokedByEmpty =
                TlsResourceBuilder.createCertificateWithCrlDistributionPoint(clientKeyPairRevokedByEmpty,
                                                                             caPair,
                                                                             DN_CLIENT_REVOKED_BY_EMPTY,
                                                                             emptyCrlUri);

        final KeyPair clientKeyPairInvalidClr = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientCertificateInvalidClr =
                TlsResourceBuilder.createCertificateWithCrlDistributionPoint(clientKeyPairInvalidClr,
                                                                             caPair,
                                                                             DN_CLIENT_REVOKED_INVALID_CRL,
                                                                             nonExistingCrlUri);

        final KeyCertificatePair intermediateCA =
                TlsResourceBuilder.createKeyPairAndIntermediateCA(DN_INTERMEDIATE, caPair, crlUri);
        final KeyPair clientKeyPairIntermediate = TlsResourceBuilder.createRSAKeyPair();
        final X509Certificate clientCertificateIntermediate =
                TlsResourceBuilder.createCertificateWithCrlDistributionPoint(clientKeyPairIntermediate,
                                                                             intermediateCA,
                                                                             DN_CLIENT_INT,
                                                                             intermediateCrlUri);

        final KeyPair clientKeyPairExpired = TlsResourceBuilder.createRSAKeyPair();
        final Instant from = Instant.now().minus(10, ChronoUnit.DAYS);
        final Instant to = Instant.now().minus(5, ChronoUnit.DAYS);
        final X509Certificate clientCertificateExpired = TlsResourceBuilder.createCertificate(clientKeyPairExpired,
                                                                                              caPair,
                                                                                              "CN=user1",
                                                                                              from,
                                                                                              to);
        _clientExpiredKeyStore =
                TLS_RESOURCE.createKeyStore(
                        new PrivateKeyEntry("user1",
                                            clientKeyPairExpired.getPrivate(),
                                            clientCertificateExpired,
                                            caPair.getCertificate())).toFile().getAbsolutePath();

        _clientKeyStore = TLS_RESOURCE.createKeyStore(
                new PrivateKeyEntry(CERT_ALIAS_APP1,
                                    clientApp1KeyPair.getPrivate(),
                                    clientApp1Certificate,
                                    caPair.getCertificate()),
                new PrivateKeyEntry(CERT_ALIAS_APP2,
                                    clientApp2KeyPair.getPrivate(),
                                    clientApp2Certificate,
                                    caPair.getCertificate()),
                new PrivateKeyEntry(CERT_ALIAS_ALLOWED,
                                    clientAllowedKeyPair.getPrivate(),
                                    clientAllowedCertificate,
                                    caPair.getCertificate()),
                new PrivateKeyEntry(CERT_ALIAS_REVOKED,
                                    clientRevokedKeyPair.getPrivate(),
                                    clientRevokedCertificate,
                                    caPair.getCertificate()),
                new PrivateKeyEntry(CERT_ALIAS_REVOKED_EMPTY_CRL,
                                    clientKeyPairRevokedByEmpty.getPrivate(),
                                    clientCertificateRevokedByEmpty,
                                    caPair.getCertificate()),
                new PrivateKeyEntry(CERT_ALIAS_REVOKED_INVALID_CRL_PATH,
                                    clientKeyPairInvalidClr.getPrivate(),
                                    clientCertificateInvalidClr,
                                    caPair.getCertificate()),
                new PrivateKeyEntry(CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE,
                                    clientKeyPairIntermediate.getPrivate(),
                                    clientCertificateIntermediate,
                                    intermediateCA.getCertificate(),
                                    caPair.getCertificate()),
                new CertificateEntry(CERT_ALIAS_ROOT_CA, caPair.getCertificate())).toFile().getAbsolutePath();

        _clientTrustStore = TLS_RESOURCE.createKeyStore(new CertificateEntry(CERT_ALIAS_ROOT_CA,
                                                                             caPair.getCertificate()))
                                        .toFile()
                                        .getAbsolutePath();

        final Path crl = TLS_RESOURCE.createCrlAsDer(caPair, clientRevokedCertificate, intermediateCA.getCertificate());
        Files.copy(crl, _crlFile, StandardCopyOption.REPLACE_EXISTING);

        final Path emptyCrl = TLS_RESOURCE.createCrlAsDer(caPair);
        Files.copy(emptyCrl, _emptyCrlFile, StandardCopyOption.REPLACE_EXISTING);

        final Path intermediateCrl = TLS_RESOURCE.createCrlAsDer(caPair);
        Files.copy(intermediateCrl, _intermediateCrlFile, StandardCopyOption.REPLACE_EXISTING);

        final KeyCertificatePair clientKeyPairUntrusted = TlsResourceBuilder.createSelfSigned(DN_CLIENT_UNTRUSTED);
        _clientUntrustedKeyStore = TLS_RESOURCE.createKeyStore(
                new PrivateKeyEntry(CERT_ALIAS_APP1,
                                    clientKeyPairUntrusted.getPrivateKey(),
                                    clientKeyPairUntrusted.getCertificate()))
                                               .toFile()
                                               .getAbsolutePath();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        System.clearProperty("javax.net.debug");
        if (getProtocol() != Protocol.AMQP_1_0)
        {
            System.clearProperty("amqj.MaximumStateWait");
        }

        CRL_SERVER.stop();
    }


    @Test
    public void md5() throws Exception
    {
        assumeThat("Qpid JMS Client does not support MD5 mechanisms",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        final int port = createAuthenticationProviderAndUserAndPort(getTestName(), "MD5");

        assertPlainConnectivity(port, CramMd5HashedNegotiator.MECHANISM);
    }

    @Test
    public void sha256() throws Exception
    {
        final int port = createAuthenticationProviderAndUserAndPort(getTestName(),
                                                                    ScramSHA256AuthenticationManager.PROVIDER_TYPE);

        assertPlainConnectivity(port, ScramSHA256AuthenticationManager.MECHANISM);
    }

    @Test
    public void sha1() throws Exception
    {
        final int port = createAuthenticationProviderAndUserAndPort(getTestName(),
                                                                    ScramSHA1AuthenticationManager.PROVIDER_TYPE);

        assertPlainConnectivity(port, ScramSHA1AuthenticationManager.MECHANISM);
    }

    @Test
    public void external() throws Exception
    {

        final int port = createExternalProviderAndTlsPort(getBrokerTrustStoreAttributes(), null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithDataUrlCrlFileAndAllowedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, createDataUrlForFile(_crlFile));
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithDataUrlCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, createDataUrlForFile(_crlFile));
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertNoTlsConnectivity(port, CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationWithCrlFileAndAllowedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithCrlFileAndAllowedCertificateWithoutPreferCrls() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST,
                                 false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertNoTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertNoTlsConnectivity(port, CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationWithEmptyCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL,
                                 _emptyCrlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndAllowedCertificateWithCrlUrl() throws Exception
    {
        assumeThat(getJvmVendor(), Matchers.not(JvmVendor.IBM));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrl() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertNoTlsConnectivity(port, CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithEmptyCrl() throws Exception
    {
        assumeThat(getJvmVendor(), Matchers.not(JvmVendor.IBM));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_REVOKED_EMPTY_CRL);
    }

    @Test
    public void externalWithRevocationDisabledWithCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationDisabledWithCrlUrlInRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithSoftFail() throws Exception
    {
        assumeThat(getJvmVendor(), Matchers.not(JvmVendor.IBM));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_REVOKED_INVALID_CRL_PATH);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithoutPreferCrls() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST,
                                 false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertNoTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithoutPreferCrlsWithFallback() throws Exception
    {
        assumeThat(getJvmVendor(), Matchers.not(JvmVendor.IBM));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST,
                                 false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_NO_FALLBACK, false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndRevokedIntermediateCertificateWithCrlUrl() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertNoTlsConnectivity(port, CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE);
    }

    @Test
    public void externalWithRevocationAndRevokedIntermediateCertificateWithCrlUrlOnlyEndEntity() throws Exception
    {
        assumeThat(getJvmVendor(), Matchers.not(JvmVendor.IBM));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE);
    }

    @Test
    public void externalDeniesUntrustedClientCert() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        final int port = createExternalProviderAndTlsPort(getBrokerTrustStoreAttributes(), null, false);

        try
        {
            getConnectionBuilder(port, CERT_ALIAS_UNTRUSTED_CLIENT).setKeyStoreLocation(_clientUntrustedKeyStore)
                                                                   .build()
                                                                   .close();
            fail("Should not be able to create a connection to the SSL port");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    @Test
    public void externalDeniesExpiredClientCert() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerPeerStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret());
        trustStoreAttributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setTls(true)
                                  .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                  .setKeyStoreLocation(_clientExpiredKeyStore)
                                  .setKeyStorePassword(TLS_RESOURCE.getSecret())
                                  .setTrustStoreLocation(_clientTrustStore)
                                  .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                  .build();
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    @Test
    public void externalWithPeersOnlyTrustStore() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerPeerStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret());
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        assertTlsConnectivity(port, CERT_ALIAS_APP1);

        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));
        assertNoTlsConnectivity(port, CERT_ALIAS_APP2);
    }

    @Test
    public void externalWithRegularAndPeersOnlyTrustStores() throws Exception
    {
        final String trustStoreName = getTestName() + "RegularTrustStore";

        try (final BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                              new AmqpManagementFacade(getProtocol())))
        {
            final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
            helper.openManagementConnection()
                  .createEntity(trustStoreName, FileTrustStore.class.getName(), trustStoreAttributes);
        }

        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerPeerStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret());
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, trustStoreName, false);
        assertTlsConnectivity(port, CERT_ALIAS_APP1);

        //use the app2 cert, which is NOT in the peerstore (but is signed by the same CA as app1)
        assertTlsConnectivity(port, CERT_ALIAS_APP2);
    }

    @Test
    public void externalUsernameAsDN() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();

        final String clientId = getTestName();
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, true);
        final Connection connection = getConnectionBuilder(port, CERT_ALIAS_APP2).setClientId(clientId).build();
        try
        {
            try (final BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                                  new AmqpManagementFacade(getProtocol())))
            {
                String principal =
                        helper.openManagementConnection().getConnectionPrincipalByClientId(getPortName(), clientId);
                assertEquals("Unexpected principal", "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA", principal);
            }
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void externalUsernameAsCN() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();

        final String clientId = getTestName();
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        final Connection connection = getConnectionBuilder(port, CERT_ALIAS_APP2).setClientId(clientId).build();
        try
        {
            try (final BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                                  new AmqpManagementFacade(getProtocol())))
            {
                String principal =
                        helper.openManagementConnection().getConnectionPrincipalByClientId(getPortName(), clientId);
                assertEquals("Unexpected principal", "app2@acme.org", principal);
            }
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }
        finally
        {
            connection.close();
        }
    }

    private Map<String, Object> getBrokerTrustStoreAttributes()
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerTrustStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret());
        trustStoreAttributes.put(FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());
        return trustStoreAttributes;
    }

    private int createExternalProviderAndTlsPort(final Map<String, Object> trustStoreAttributes,
                                                 final String additionalTrustStore,
                                                 final boolean useFullDN) throws Exception
    {
        final String providerName = getTestName();
        final String keyStoreName = providerName + "KeyStore";
        final String trustStoreName = providerName + "TrustStore";
        final String portName = getPortName();
        final Map<String, Object> trustStoreSettings = new HashMap<>(trustStoreAttributes);

        final String[] trustStores = additionalTrustStore == null
                ? new String[]{trustStoreName}
                : new String[]{trustStoreName, additionalTrustStore};

        try (BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                        new AmqpManagementFacade(getProtocol())))
        {
            return helper.openManagementConnection()
                         .createExternalAuthenticationProvider(providerName, useFullDN)
                         .createKeyStore(keyStoreName, _brokerKeyStore, TLS_RESOURCE.getSecret())
                         .createEntity(trustStoreName, FileTrustStore.class.getName(), trustStoreSettings)
                         .createAmqpTlsPort(portName, providerName, keyStoreName, false, true, false, trustStores)
                         .getAmqpBoundPort(portName);
        }
    }

    private String getPortName()
    {
        return getTestName() + "TlsPort";
    }

    private int createAuthenticationProviderAndUserAndPort(final String providerName,
                                                           final String providerType) throws Exception
    {

        final String portName = providerName + "Port";
        final Map<String, Object> portAttributes = new HashMap<>();
        portAttributes.put(Port.AUTHENTICATION_PROVIDER, providerName);
        portAttributes.put(Port.PORT, 0);

        try (BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                        new AmqpManagementFacade(getProtocol())))
        {
            return helper.openManagementConnection()
                         .createAuthenticationProvider(providerName, providerType)
                         .createUser(providerName, USER, USER_PASSWORD)
                         .createEntity(portName, "org.apache.qpid.AmqpPort", portAttributes)
                         .getAmqpBoundPort(portName);
        }
    }

    private Connection getConnection(int port, String certificateAlias) throws NamingException, JMSException
    {
        return getConnectionBuilder(port, certificateAlias).build();
    }

    private ConnectionBuilder getConnectionBuilder(int port, String certificateAlias)
    {
        return getConnectionBuilder().setPort(port)
                                     .setTls(true)
                                     .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                     .setKeyStoreLocation(_clientKeyStore)
                                     .setKeyStorePassword(TLS_RESOURCE.getSecret())
                                     .setKeyAlias(certificateAlias)
                                     .setTrustStoreLocation(_clientTrustStore)
                                     .setTrustStorePassword(TLS_RESOURCE.getSecret());
    }

    private void assertTlsConnectivity(int port, String certificateAlias) throws NamingException, JMSException
    {
        final Connection connection = getConnection(port, certificateAlias);
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Temporary queue was not created", session.createTemporaryQueue());
        }
        finally
        {
            connection.close();
        }
    }

    private void assertNoTlsConnectivity(int port, String certificateAlias) throws NamingException
    {
        try
        {
            getConnection(port, certificateAlias);
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }


    private void assertPlainConnectivity(final int port,
                                         final String mechanism) throws Exception
    {
        final Connection connection = getConnectionBuilder().setPort(port)
                                                            .setUsername(USER)
                                                            .setPassword(USER_PASSWORD)
                                                            .setSaslMechanisms(mechanism)
                                                            .build();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryQueue queue = session.createTemporaryQueue();
            assertNotNull("Temporary queue was not created", queue);
        }
        finally
        {
            connection.close();
        }

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setUsername(USER)
                                  .setPassword("invalid" + USER_PASSWORD)
                                  .setSaslMechanisms(mechanism)
                                  .build();
            fail("Connection is established for invalid password");
        }
        catch (JMSException e)
        {
            // pass
        }

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setUsername("invalid" + AuthenticationTest.USER)
                                  .setPassword(USER_PASSWORD)
                                  .setSaslMechanisms(mechanism)
                                  .build();
            fail("Connection is established for invalid user name");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    private static void createContext(Path crlPath)
    {
        final ContextHandler contextHandler = new ContextHandler();
        contextHandler.setContextPath("/" + crlPath.getFileName());
        contextHandler.setHandler(new CrlServerHandler(crlPath));
        HANDLERS.addHandler(contextHandler);
    }


    public static String createDataUrlForFile(Path file) throws IOException
    {
        return DataUrlUtils.getDataUrlForBytes(Files.readAllBytes(file));
    }

    private static class CrlServerHandler extends AbstractHandler
    {
        final Path crlPath;

        CrlServerHandler(Path crlPath)
        {
            this.crlPath = crlPath;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            final byte[] crlBytes = Files.readAllBytes(crlPath);
            response.setStatus(HttpServletResponse.SC_OK);
            try (final OutputStream responseBody = response.getOutputStream())
            {
                responseBody.write(crlBytes);
            }
        }
    }
}
