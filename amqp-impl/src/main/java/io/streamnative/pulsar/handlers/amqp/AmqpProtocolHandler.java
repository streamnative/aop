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
package io.streamnative.pulsar.handlers.amqp;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;
import io.streamnative.pulsar.handlers.amqp.utils.ConfigurationUtils;
import java.net.InetSocketAddress;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

/**
 * Amqp Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class AmqpProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "amqp";
    public static final String SSL_PREFIX = "amqp+ssl://";
    public static final String PLAINTEXT_PREFIX = "amqp://";
    public static final String LISTENER_DEL = ",";
    public static final String LISTENER_PATTEN = "^(amqp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-0-9+]";

    @Getter
    private AmqpServiceConfiguration amqpConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private String bindAddress;
    @Getter
    private AmqpBrokerService amqpBrokerService;
    private Server webServer;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof AmqpServiceConfiguration) {
            // in unit test, passed in conf will be AmqpServiceConfiguration
            amqpConfig = (AmqpServiceConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            amqpConfig = ConfigurationUtils.create(conf.getProperties(), AmqpServiceConfiguration.class);
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(amqpConfig.getBindAddress());
    }

    // This method is called after initialize
    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listeners: {}", getAppliedAmqpListeners(amqpConfig));
        }
        return getAppliedAmqpListeners(amqpConfig);
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        amqpBrokerService = new AmqpBrokerService(service.getPulsar());
        if (amqpConfig.isAmqpProxyEnable()) {
            ProxyConfiguration proxyConfig = new ProxyConfiguration();
            proxyConfig.setAmqpTenant(amqpConfig.getAmqpTenant());
            proxyConfig.setAmqpMaxNoOfChannels(amqpConfig.getAmqpMaxNoOfChannels());
            proxyConfig.setAmqpMaxFrameSize(amqpConfig.getAmqpMaxFrameSize());
            proxyConfig.setAmqpHeartBeat(amqpConfig.getAmqpHeartBeat());
            proxyConfig.setAmqpProxyPort(amqpConfig.getAmqpProxyPort());
            proxyConfig.setAmqpExplicitFlushAfterFlushes(amqpConfig.getAmqpExplicitFlushAfterFlushes());

            AdvertisedListener internalListener = ServiceConfigurationUtils.getInternalListener(amqpConfig, "pulsar");
            checkArgument(internalListener.getBrokerServiceUrl() != null,
                    "plaintext must be configured on internal listener");
            proxyConfig.setBrokerServiceURL(internalListener.getBrokerServiceUrl().toString());

            ProxyService proxyService = new ProxyService(proxyConfig, service.getPulsar());
            try {
                proxyService.start();
                log.info("Start amqp proxy service at port: {}", proxyConfig.getAmqpProxyPort());
            } catch (Exception e) {
                log.error("Failed to start amqp proxy service.");
            }
        }
        startAdminResource(amqpConfig.getAmqpAdminPort());
        log.info("Starting AmqpProtocolHandler, listener: {}, aop version is: '{}'",
                getAppliedAmqpListeners(amqpConfig), AopVersion.getVersion());
        log.info("Git Revision {}", AopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
            AopVersion.getBuildUser(),
            AopVersion.getBuildHost(),
            AopVersion.getBuildTime());
    }

    private void startAdminResource(int port) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/api");
        context.setAttribute("aop", this);

        webServer = new Server(port);
        webServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.packages", "io.streamnative.pulsar.handlers.amqp.admin");

        try {
            webServer.start();
        } catch (Exception e) {
            log.error("Failed to start web service for aop", e);
        }
    }

    // this is called after initialize, and with amqpConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(amqpConfig != null);
        checkState(getAppliedAmqpListeners(amqpConfig) != null);
        checkState(brokerService != null);

        String listeners = getAppliedAmqpListeners(amqpConfig);
        String[] parts = listeners.split(LISTENER_DEL);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                        new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                        new AmqpChannelInitializer(amqpConfig, amqpBrokerService));
                } else {
                    log.error("Amqp listener {} not supported. supports {} and {}",
                        listener, PLAINTEXT_PREFIX, SSL_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e){
            log.error("AmqpProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        try {
            webServer.stop();
        } catch (Exception e) {
            log.error("Failed to stop web server for aop", e);
        }
    }

    public static int getListenerPort(String listener) {
        checkState(listener.matches(LISTENER_PATTEN), "listener not match patten");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    public static String getAppliedAmqpListeners(AmqpServiceConfiguration configuration) {
        String amqpListeners = configuration.getAmqpListeners();
        if (amqpListeners == null) {
            String fullyHostName = ServiceConfigurationUtils.unsafeLocalhostResolve();
            return amqpUrl(fullyHostName, 5672);
        }
        return amqpListeners;
    }

    public static String amqpUrl(String host, int port) {
        return String.format("amqp://%s:%d", host, port);
    }
}
