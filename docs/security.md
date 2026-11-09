# Security

AoP supports authentication, authority.

## Authentication

By default, AoP is installed with no encryption, authentication, and authorization. You can enable the authentication feature for AoP to improve security. Currently, this feature is **only available in Java**.

AoP authentication mechanism uses PLAIN mechanisms and achieves authentication with [Pulsar token-based authentication mechanism](https://pulsar.apache.org/docs/en/security-overview/). Consequently, if you want to enable the authentication feature for AoP, you need to enable authentication for the following components:

- Pulsar brokers
- Aop (some configurations of AoP rely on the configurations of Pulsar brokers)
- Rabbitmq clients

Currently, PLAIN mechanism based on JWT authentication, so you must configure `authenticationProviders` with `AuthenticationProviderToken`. See following chapters for more details.

### `PLAIN`

Currently, in order to improve security, we used SASL_PLAIN mechanism in KoP to Implement PLAIN mechanism in AoP, and it's a better way for authorizartion.

If you want to enable the authentication feature for AoP using the `PLAIN` mechanism, follow the steps below.

1. Enable authentication on Pulsar broker.

   For the `PLAIN` mechanism, the RabbitMQ authentication is forwarded to the [JWT authentication](https://pulsar.apache.org/docs/en/security-jwt/) of Pulsar, so you need to configure the JWT authentication and set the following properties in the `conf/broker.conf` or `conf/standalone.conf` file.

   (1) Enable authentication for the Pulsar broker.

   ```bash
   authenticationEnabled=true
   amqpAuthenticationEnabled=true
   authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken
   ```

   (2) Enable authentication between Pulsar broker and AoP.

   ```bash
   brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
   brokerClientAuthenticationParameters=token:<token-of-super-user-role>
   superUserRoles=<super-user-roles>
   ```

   (3) Specify the key.

   For more information, see [Enable Token Authentication on Brokers](https://pulsar.apache.org/docs/en/next/security-jwt/#enable-token-authentication-on-brokers).

   - If you use a secret key, set the property as below.

   ```bash
   tokenSecretKey=file:///path/to/secret.key
   ```

   The key can also be passed inline.

   ```bash
   tokenSecretKey=data:;base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=
   ```

   - If you use a public/private key, set the property as below.

   ```bash
   tokenPublicKey=file:///path/to/public.key
   ```
   
2. Enable authentication on AoP.

    Set the following property in the `conf/broker.conf` or `conf/standalone.conf` file.
    The client will use spaces as separators to parse this parameter and determine the authentication methods supported by the server, please use spaces as separators.

    ```properties
    amqpAllowedMechanisms=PLAIN token
    ```
   
4. Enable authentication on RabbitMQ client


| Property   | Description                                                  | Example value |
| ---------- | ------------------------------------------------------------ | :------------ |
| `password` | `password` must be your token authentication parameters from Pulsar.  The token can be created by Pulsar token tools. The role is the `subject` for the token. It is embedded in the created token and the broker can get `role` by parsing this token. | `token:xxx`   |

```java
connectionFactory.setPort(5672);
connectionFactory.setPassword("token:xxxx");
Connection connection = connectionFactory.newConnection();
```

## Authorization

Currently, AoP authorization only supports production and consumption permissions at the vhost/namespace level. To enable authorization in AoP, please make sure the authentication is enabled.

**Note**: For more information, see [Authorization](http://pulsar.apache.org/docs/en/security-jwt/#authorization).

1. Enable authorization and assign superusers for the Pulsar broker.

   ```java
   authorizationEnabled=true
   amqpAuthorizationEnabled=true
   ```

2. Generate JWT tokens.

   A token is the credential associated with a user. The association is done through the "`principal`" or "`role`". In the case of JWT tokens, this field is typically referred as `subject`, though they are exactly the same concept. Then, you need to use this command to require the generated token to have a `subject` field set.

   ```bash
   $ bin/pulsar tokens create --secret-key file:///path/to/secret.key \
        --subject <user-role>
   ```

   This command prints the token string on stdout.

3. Grant permission to specific role.

   The token itself does not have any permission associated. The authorization engine determines whether the token should have permissions or not. Once you have created the token, you can grant permission for this token to do certain actions.
   The following is an example.

   ```bash
   $ bin/pulsar-admin --auth-plugin "org.apache.pulsar.client.impl.auth.AuthenticationToken" --auth-params "token:<token-of-super-user-role>" \
            namespaces grant-permission <tenant>/<namespace> \
            --role <user-role> \
            --actions produce,consume
   ```
   
## SSL connection

Currently, TLS/SSL in Aop is only designed for proxy port, and the proxy port ensures correct addressing of cluster brokers

The following example shows how to connect AoP through TLS/SSL , and for user, client is used just like Rabbitmq.

1. Using tls-gen's Basic Profile

```bash
git clone https://github.com/rabbitmq/tls-gen tls-gen
cd tls-gen/basic
# private key password
make PASSWORD=123456
make verify
make info
ls -l ./result
```

The certificate chain produced by this basic tls-gen profile looks like this:

![Root CA and leaf certificates](https://www.rabbitmq.com/img/root_ca_and_leaf.png)

The files generated in the above steps can already ensure the normal operation of one-way authentication, but to ensure two-way authentication, the following two files need to be generated. Java will use TrustStore to trust and uniformly manage related files. Below, we create two trust files, one containing the trust chain information of the CA and the other containing the server certificate used by the client

```bash
keytool -import -alias server1 -file /path/to/ca_certificate.pem -keystore /path/to/rabbitstore_ca
keytool -import -alias server1 -file /path/to/server_certificate.pem -keystore /path/to/rabbitstore
```

We will have 10 files below:

- `client` for `client.p12` file, which is including `client_certificate.pem` file and  `client_key.pem` file
- `server` for `server.p12` file, which is including `server_certificate.pem` file and  `server_key.pem` file
- `CA` for `ca_certificate.pem` file and  `ca_key.pem` file
- `rabbitstore_ca` for trusted  `ca_certificate.pem` file, including the trusted certificate chain.
- `rabbitstore` for trusted`server_certificate.pem` file, including the trusted server certificate.

2. Enableing TLS Support in Aop

Here are the essential configuration settings related to TLS:

| Configuration Key            | Description                                                  |
| ---------------------------- | ------------------------------------------------------------ |
| amqpTlsEnabled               | Aop Tls Enable, true or false                                |
| amqpSslProtocol              | The protocol server sending to the client, default TLSv1.2   |
| amqpSslKeystoreType          | The certification type Server Keystore used, default PKCS12  |
| amqpSslKeystoreLocation      | The Server Keystore p12 file location, example /etc/ssl/server.p12 |
| amqpSslKeystorePassword      | The Server Keystore p12 file password                        |
| amqpSslTruststoreType        | The certification type Truststore(CA) used, default JKS      |
| amqpSslTruststoreLocation    | The Truststore(CA) file location, example /etc/ssl/rabbitstore_ca |
| amqpSslTruststorePassword    | The Truststore(CA) file password                             |
| amqpSslKeymanagerAlgorithm   | The Keymanager algorithm, default SunX509                    |
| amqpSslTrustmanagerAlgorithm | The Trustmanager algorithm, default SunX509                  |
| amqpSslClientAuth            | When set to true, TLS connection will be peer                |

3. Configure the RabbitMQ client.

The usage of the client depends on whether `amqpSslClientAuth`  are turned on or not:

- `amqpSslClientAuth`  = ` false`

```java
        connectionFactory.setPort(6672);
        connectionFactory.useSslProtocol("TLSv1.2");
        Connection connection = connectionFactory.newConnection();
```

- `amqpSslClientAuth`  = ` true`

```java
        connectionFactory.setPort(6672);

        char[] keyPassphrase = "Keystore file password".toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(new FileInputStream("The client p12 Keystore file location"), keyPassphrase);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks,keyPassphrase);

        char[] trustPassphrase = "Truststore file password".toCharArray();
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load(new FileInputStream("The Truststore p12 file location"), trustPassphrase);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(tks);

        SSLContext c = SSLContext.getInstance("TLSv1.2");
        c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        connectionFactory.useSslProtocol(c);
        Connection connection = connectionFactory.newConnection();
```