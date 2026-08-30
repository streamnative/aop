package io.streamnative.pulsar.handlers.amqp.utils;

import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 * Helper class for setting up SSL for KafkaChannelInitializer.
 * RabbitMQ and Pulsar use different way to store SSL keys, this utils only work for RabbitMQ.
 */
public class SSLUtils {

    /**
     * Create SSL engine used in ServiceChannelInitializer.
     */
    public static SSLEngine createSslEngineSun(ProxyConfiguration proxyConfig) throws Exception {
        KeyManagerFactory kmf = createKeyManagerFactory(proxyConfig);
        TrustManagerFactory tmf = createTrustManagerFactory(proxyConfig);

        SSLContext sslContext = SSLContext.getInstance(proxyConfig.getAmqpSslProtocol());
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        // It means engine working for AMQP server not AMQP client
        sslEngine.setUseClientMode(false);

        setClientAuth(sslEngine, proxyConfig.isAmqpSslClientAuth());
        return sslEngine;
    }

    /**
     * Create a KeyManager to manage the KeyStore containing the server p12 file
     * */
    public static KeyManagerFactory createKeyManagerFactory(ProxyConfiguration proxyConfig) throws Exception {
        char[] keyPassphrase = proxyConfig.getAmqpSslKeystorePassword().toCharArray();
        KeyStore ks = KeyStore.getInstance(proxyConfig.getAmqpSslKeystoreType());
        ks.load(new FileInputStream(proxyConfig.getAmqpSslKeystoreLocation()), keyPassphrase);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(proxyConfig.getAmqpSslKeymanagerAlgorithm());
        kmf.init(ks, keyPassphrase);
        return kmf;
    }

    /**
     * Create a TrustManager to manage the TrustStore trusting the CA certificates
     * */
    public static TrustManagerFactory createTrustManagerFactory(ProxyConfiguration proxyConfig) throws Exception {
        char[] trustPassphrase = proxyConfig.getAmqpSslTruststorePassword().toCharArray();
        KeyStore tks = KeyStore.getInstance(proxyConfig.getAmqpSslTruststoreType());
        tks.load(new FileInputStream(proxyConfig.getAmqpSslTruststoreLocation()), trustPassphrase);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(proxyConfig.getAmqpSslTrustmanagerAlgorithm());
        tmf.init(tks);
        return tmf;
    }

    /**
     * If true, client wound be asked to auth to make TLS HAND_SHAKE.
     * If false, client don't need to take relevant files to make TLS HAND_SHAKE
     * */
    public static void setClientAuth(SSLEngine sslEngine, boolean isPeer) {
        sslEngine.setNeedClientAuth(isPeer);
    }
}
