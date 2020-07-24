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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;

/**
 * TlsHelper.
 */
public class TlsHelper
{
    private static final String DN_CA = "CN=MyRootCA,O=ACME,ST=Ontario,C=CA";
    private static final String DN_BROKER = "CN=localhost,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown";
    private static final String DN_CLIENT_APP1 = "CN=app1@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_APP2 = "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String CERT_ALIAS_ROOT_CA = "rootca";
    public static final String CERT_ALIAS_APP1 = "app1";
    public static final String CERT_ALIAS_APP2 = "app2";
    private static final String BROKER_ALIAS = "java-broker";

    private Path _brokerKeyStore;
    private Path _brokerTrustStore;
    private Path _clientKeyStore;
    private Path _clientTrustStore;
    private X509Certificate _caCertificate;
    private KeyCertificatePair _clientKeyPair1;
    private final KeyCertificatePair _caPair;

    public TlsHelper(TlsResource tlsResource) throws Exception
    {
        _caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final KeyPair brokerKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final KeyPair clientKeyPair1 = TlsResourceBuilder.createRSAKeyPair();
        final KeyPair clientKeyPair2 = TlsResourceBuilder.createRSAKeyPair();

        final X509Certificate brokerCertificate =
                TlsResourceBuilder.createCertificateForServerAuthorization(brokerKeyPair, _caPair, DN_BROKER);
        final X509Certificate clientCertificate1 =
                TlsResourceBuilder.createCertificateForClientAuthorization(clientKeyPair1, _caPair, DN_CLIENT_APP1);
        final X509Certificate clientCertificate2 =
                TlsResourceBuilder.createCertificateForClientAuthorization(clientKeyPair2, _caPair, DN_CLIENT_APP2);

        final PrivateKey privateKey = clientKeyPair1.getPrivate();
        final X509Certificate certificate = clientCertificate1;
        _clientKeyPair1 = new KeyCertificatePair(privateKey, certificate);
        _caCertificate = _caPair.getCertificate();

        _brokerKeyStore = tlsResource.createKeyStore(new PrivateKeyEntry(BROKER_ALIAS,
                                                                         brokerKeyPair.getPrivate(),
                                                                         brokerCertificate,
                                                                         _caCertificate));
        _brokerTrustStore = tlsResource.createKeyStore(new CertificateEntry(CERT_ALIAS_ROOT_CA,
                                                                            _caCertificate));
        _clientKeyStore =
                tlsResource.createKeyStore(new PrivateKeyEntry(CERT_ALIAS_APP1,
                                                               clientKeyPair1.getPrivate(),
                                                               clientCertificate1,
                                                               _caCertificate),
                                           new PrivateKeyEntry(CERT_ALIAS_APP2,
                                                               clientKeyPair2.getPrivate(),
                                                               clientCertificate2,
                                                               _caCertificate));

        _clientTrustStore = tlsResource.createKeyStore(new CertificateEntry(CERT_ALIAS_ROOT_CA,
                                                                            _caCertificate));
    }


    public String getClientKeyStore()
    {
        return _clientKeyStore.toFile().getAbsolutePath();
    }

    public String getClientTrustStore()
    {
        return _clientTrustStore.toFile().getAbsolutePath();
    }

    public X509Certificate getCaCertificate()
    {
        return _caCertificate;
    }

    public PrivateKey getClientPrivateKey()
    {
        return _clientKeyPair1.getPrivateKey();
    }

    public X509Certificate getClientCerificate()
    {
        return _clientKeyPair1.getCertificate();
    }

    public String getBrokerKeyStore()
    {
        return _brokerKeyStore.toFile().getAbsolutePath();
    }

    public String getBrokerTrustStore()
    {
        return _brokerTrustStore.toFile().getAbsolutePath();
    }

    public KeyCertificatePair getCaKeyCertPair()
    {
        final PrivateKey privateKey = _caPair.getPrivateKey();
        final X509Certificate certificate = _caCertificate;
        return new KeyCertificatePair(privateKey, certificate);
    }
}
