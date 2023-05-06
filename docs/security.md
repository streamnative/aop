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

Currently, in order to improve security, we used SASL_PLAIN mechanism in KOP to Implement PLAIN mechanism in AOP, and it's a better way for authorizartion.

If you want to enable the authentication feature for AoP using the `PLAIN` mechanism, follow the steps below.

1. Enable authentication on Pulsar broker.

   For the `PLAIN` mechanism, the Rabbitmq authentication is forwarded to the [JWT authentication](https://pulsar.apache.org/docs/en/security-jwt/) of Pulsar, so you need to configure the JWT authentication and set the following properties in the `conf/broker.conf` or `conf/standalone.conf` file.

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
   
4. Enable authentication on Rabbitmq client


| Property   | Description                                                  | Example value |
| ---------- | ------------------------------------------------------------ | :------------ |
| `password` | `password` must be your token authentication parameters from Pulsar.  The token can be created by Pulsar token tools. The role is the `subject` for the token. It is embedded in the created token and the broker can get `role` by parsing this token. | `token:xxx`   |

```java
connectionFactory.setPort(5672);
connectionFactory.setPassword("token:xxxx");
Connection connection = connectionFactory.newConnection();
```



## Authorization

Currently, AOP authorization only supports production and consumption permissions at the vhost/namespace level. To enable authorization in AoP, please make sure the authentication is enabled.

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