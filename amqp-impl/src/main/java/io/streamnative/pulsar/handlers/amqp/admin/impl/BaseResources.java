/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.admin.impl;

import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.ExchangeService;
import io.streamnative.pulsar.handlers.amqp.QueueContainer;
import io.streamnative.pulsar.handlers.amqp.QueueService;
import io.streamnative.pulsar.handlers.amqp.admin.AmqpAdmin;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.TenantBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import io.streamnative.pulsar.handlers.amqp.admin.prometheus.PrometheusAdmin;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.qpid.server.protocol.v0_8.FieldTable;

/**
 * Base resources.
 */
@Slf4j
public class BaseResources {

    @HeaderParam("tenant")
    protected String tenant;

    @HeaderParam("x-vhost")
    protected String xVhost;

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private AmqpProtocolHandler protocolHandler;

    private NamespaceService namespaceService;

    private NamespaceResources namespaceResources;
    private TenantResources tenantResources;

    private ExchangeService exchangeService;

    private ExchangeContainer exchangeContainer;

    private QueueService queueService;

    private QueueContainer queueContainer;

    private ManagedLedgerFactoryImpl managedLedgerFactory;

    private PrometheusAdmin prometheusAdmin;
    private AmqpAdmin amqpAdmin;
    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;

    protected AmqpProtocolHandler aop() {
        if (protocolHandler == null) {
            protocolHandler = (AmqpProtocolHandler) servletContext.getAttribute("aop");
        }
        return protocolHandler;
    }

    protected PrometheusAdmin prometheusAdmin() {
        if (prometheusAdmin == null) {
            prometheusAdmin = aop().getAmqpBrokerService().getPrometheusAdmin();
        }
        return prometheusAdmin;
    }
   protected PulsarAdmin pulsarAdmin() {
        if (pulsarAdmin == null) {
            try {
                pulsarAdmin = aop().getBrokerService().getPulsar().getAdminClient();
            } catch (PulsarServerException e) {
                throw new AoPServiceRuntimeException(e);
            }
        }
        return pulsarAdmin;
    }

   protected PulsarClient pulsarClient() {
        if (pulsarClient == null) {
            try {
                pulsarClient = aop().getBrokerService().getPulsar().getClient();
            } catch (PulsarServerException e) {
                throw new AoPServiceRuntimeException(e);
            }
        }
        return pulsarClient;
    }

    protected AmqpAdmin amqpAdmin() {
        if (amqpAdmin == null) {
            amqpAdmin = aop().getAmqpBrokerService().getAmqpAdmin();
        }
        return amqpAdmin;
    }

    protected ManagedLedgerFactoryImpl managedLedgerFactory() {
        if (managedLedgerFactory == null) {
            managedLedgerFactory = (ManagedLedgerFactoryImpl) aop().getBrokerService().getManagedLedgerFactory();
        }
        return managedLedgerFactory;
    }


    protected NamespaceService namespaceService() {
        if (namespaceService == null) {
            namespaceService = aop().getBrokerService().getPulsar().getNamespaceService();
        }
        return namespaceService;
    }

    protected NamespaceResources namespaceResource() {
        if (namespaceResources == null) {
            namespaceResources = aop().getBrokerService().getPulsar().getPulsarResources().getNamespaceResources();
        }
        return namespaceResources;
    }

    protected TenantResources tenantResources() {
        if (tenantResources == null) {
            tenantResources = aop().getBrokerService().getPulsar().getPulsarResources().getTenantResources();
        }
        return tenantResources;
    }

    protected ExchangeService exchangeService() {
        if (exchangeService == null) {
            exchangeService = aop().getAmqpBrokerService().getExchangeService();
        }
        return exchangeService;
    }

    protected ExchangeContainer exchangeContainer() {
        if (exchangeContainer == null) {
            exchangeContainer = aop().getAmqpBrokerService().getExchangeContainer();
        }
        return exchangeContainer;
    }


    protected QueueService queueService() {
        if (queueService == null) {
            queueService = aop().getAmqpBrokerService().getQueueService();
        }
        return queueService;
    }

    protected QueueContainer queueContainer() {
        if (queueContainer == null) {
            queueContainer = aop().getAmqpBrokerService().getQueueContainer();
        }
        return queueContainer;
    }

    protected NamespaceName getNamespaceName() {
        return getNamespaceName(null);
    }

    protected NamespaceName getNamespaceName(String vhost) {
        vhost = StringUtils.isNotBlank(vhost) ? vhost : this.xVhost;
        vhost = "/".equals(vhost) ? "default" : vhost;
        NamespaceName namespaceName;
        if (tenant == null) {
            namespaceName = NamespaceName.get(vhost);
        } else {
            namespaceName = NamespaceName.get(tenant, vhost);
        }
        return namespaceName;
    }

    protected CompletableFuture<List<VhostBean>> getVhostListAsync() {
        return namespaceResource().listNamespacesAsync(tenant)
                .thenApply(nsList -> {
                    List<VhostBean> vhostBeanList = new ArrayList<>();
                    nsList.forEach(ns -> {
                        ns = "default".equals(ns) ? "/" : ns;
                        VhostBean bean = new VhostBean();
                        bean.setName(ns);
                        vhostBeanList.add(bean);
                    });
                    return vhostBeanList;
                });
    }

    protected CompletableFuture<Map<String, String>> getTopicProperties(String namespaceName, String topicPrefix,
                                                                        String topic) {
        CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        // amqp/default/persistent/__amqp_exchange__direct_E2
        String path = namespaceName + "/persistent/" + topicPrefix + URLEncoder.encode(topic, StandardCharsets.UTF_8);
        managedLedgerFactory().getMetaStore().getManagedLedgerInfo(path, false,
                new MetaStore.MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo result, Stat stat) {
                        Map<String, String> propertiesMap = new HashMap<>();
                        if (result.getPropertiesCount() > 0) {
                            for (int i = 0; i < result.getPropertiesCount(); i++) {
                                MLDataFormats.KeyValue property = result.getProperties(i);
                                propertiesMap.put(property.getKey(), property.getValue());
                            }
                        }
                        future.complete(propertiesMap);
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                        log.error("Failed get TopicProperties.", e);
                        future.complete(null);
                    }
                });
        return future;
    }

    protected CompletableFuture<List<VhostBean>> getAllVhostListAsync() {
        return tenantResources().listTenantsAsync().thenCompose(tenantList -> {
            Stream<CompletableFuture<List<VhostBean>>> futureStream = tenantList.stream()
                    .map(s -> namespaceResource().listNamespacesAsync(s)
                            .thenApply(nsList -> {
                                List<VhostBean> vhostBeanList = new ArrayList<>();
                                nsList.forEach(ns -> {
                                    VhostBean bean = new VhostBean();
                                    bean.setName(ns);
                                    vhostBeanList.add(bean);
                                });
                                return vhostBeanList;
                            }));
            return FutureUtil.waitForAll(futureStream).thenApply(vhostBeans -> {
                vhostBeans.sort(Comparator.comparing(VhostBean::getName));
                return vhostBeans;
            });
        });
    }

    protected CompletableFuture<List<TenantBean>> getAllTenantListAsync() {
        return tenantResources().listTenantsAsync()
                .thenApply(tenantList -> tenantList.stream()
                        .map(s -> {
                            TenantBean bean = new TenantBean();
                            bean.setName(s);
                            return bean;
                        }).sorted(Comparator.comparing(TenantBean::getName))
                        .collect(Collectors.toList()));
    }

    private PulsarService pulsar() {
        return aop().getBrokerService().getPulsar();
    }

    public boolean isRequestHttps() {
        return "https".equalsIgnoreCase(httpRequest.getScheme());
    }

    protected CompletableFuture<Void> validateTopicOwnershipAsync(TopicName topicName, boolean authoritative) {
        NamespaceService nsService = pulsar().getNamespaceService();

        LookupOptions options = LookupOptions.builder()
                .authoritative(authoritative)
                .requestHttps(isRequestHttps())
                .readOnly(false)
                .loadTopicsInBundle(false)
                .build();

        return nsService.getWebServiceUrlAsync(topicName, options)
                .thenApply(webUrl -> {
                    // Ensure we get a url
                    if (webUrl == null || !webUrl.isPresent()) {
                        log.info("Unable to get web service url");
                        throw new RestException(Response.Status.PRECONDITION_FAILED,
                                "Failed to find ownership for topic:" + topicName);
                    }
                    return webUrl.get();
                }).thenCompose(webUrl -> nsService.isServiceUnitOwnedAsync(topicName)
                        .thenApply(isTopicOwned -> Pair.of(webUrl, isTopicOwned))
                ).thenAccept(pair -> {
                    URL webUrl = pair.getLeft();
                    boolean isTopicOwned = pair.getRight();

                    if (!isTopicOwned) {
                        boolean newAuthoritative = isLeaderBroker(pulsar());
                        // Replace the host and port of the current request and redirect
                        URI redirect = UriBuilder.fromUri(uri.getRequestUri())
                                .host(webUrl.getHost())
                                .port(aop().getAmqpConfig().getAmqpAdminPort())
                                .replaceQueryParam("authoritative", newAuthoritative)
                                .build();
                        // Redirect
                        if (log.isDebugEnabled()) {
                            log.debug("Redirecting the rest call to {}", redirect);
                        }
                        throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                    }
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof IllegalArgumentException
                            || ex.getCause() instanceof IllegalStateException) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to find owner for topic: {}", topicName, ex);
                        }
                        throw new RestException(Response.Status.PRECONDITION_FAILED,
                                "Can't find owner for topic " + topicName);
                    } else if (ex.getCause() instanceof WebApplicationException) {
                        throw (WebApplicationException) ex.getCause();
                    } else {
                        throw new RestException(ex.getCause());
                    }
                });
    }

    protected static boolean isLeaderBroker(PulsarService pulsar) {
        return pulsar.getLeaderElectionService().isLeader();
    }

    protected static boolean isRedirectException(Throwable ex) {
        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
        return realCause instanceof WebApplicationException
                && ((WebApplicationException) realCause).getResponse().getStatus()
                == Response.Status.TEMPORARY_REDIRECT.getStatusCode();
    }

    protected static void resumeAsyncResponseExceptionally(AsyncResponse asyncResponse, Throwable exception) {
        Throwable realCause = FutureUtil.unwrapCompletionException(exception);
        if (realCause instanceof WebApplicationException) {
            asyncResponse.resume(realCause);
        } else if (realCause instanceof BrokerServiceException.NotAllowedException) {
            asyncResponse.resume(new RestException(Response.Status.CONFLICT, realCause));
        } else if (realCause instanceof MetadataStoreException.NotFoundException) {
            asyncResponse.resume(new RestException(Response.Status.NOT_FOUND, realCause));
        } else if (realCause instanceof MetadataStoreException.BadVersionException) {
            asyncResponse.resume(new RestException(Response.Status.CONFLICT, "Concurrent modification"));
        } else if (realCause instanceof PulsarAdminException) {
            asyncResponse.resume(new RestException(((PulsarAdminException) realCause)));
        } else if (realCause instanceof AoPServiceRuntimeException.NoSuchQueueException
                || realCause instanceof AoPServiceRuntimeException.NoSuchExchangeException
                || realCause instanceof AoPServiceRuntimeException.GetMessageException) {
            asyncResponse.resume(new RestException(500, realCause.getMessage()));
        } else {
            asyncResponse.resume(new RestException(realCause));
        }
    }

    protected CompletableFuture<Void> queueBindAsync(NamespaceName namespaceName, String exchange, String queue,
                                                     BindingParams params) {
        if (aop().getAmqpConfig().isAmqpMultiBundleEnable()) {
            return exchangeService().queueBind(namespaceName, exchange, queue, params.getRoutingKey(),
                    params.getArguments());
        } else {
            return queueService().queueBind(namespaceName, queue, exchange, params.getRoutingKey(),
                    false, FieldTable.convertToFieldTable(params.getArguments()), -1);
        }
    }

    protected CompletableFuture<Void> queueBindExchange(NamespaceName namespaceName, String exchange, String queue,
                                                     BindingParams params) {
        if (aop().getAmqpConfig().isAmqpMultiBundleEnable()) {
            return queueService().queueBind(namespaceName, queue, exchange, params);
        } else {
            return queueService().queueBind(namespaceName, queue, exchange, params.getRoutingKey(),
                    false, FieldTable.convertToFieldTable(params.getArguments()), -1);
        }
    }

    protected CompletableFuture<Void> queueUnbindAsync(NamespaceName namespaceName, String exchange, String queue,
                                                       String propertiesKey) {
        if (aop().getAmqpConfig().isAmqpMultiBundleEnable()) {
            return exchangeService().queueUnBind(namespaceName, exchange, queue, propertiesKey, null);
        } else {
            return queueService().queueUnbind(namespaceName, queue, exchange, propertiesKey,
                    null, -1);
        }
    }

    protected CompletableFuture<Void> queueUnBindExchange(NamespaceName namespaceName, String exchange, String queue,
                                                       String propertiesKey) {
        if (aop().getAmqpConfig().isAmqpMultiBundleEnable()) {
            return queueService().queueUnBind(namespaceName, queue, exchange, propertiesKey);
        } else {
            return queueService().queueUnbind(namespaceName, queue, exchange, propertiesKey,
                    null, -1);
        }
    }

    public <T> List<T> getPageList(List<T> list, int page, int pageSize) {
        int p = pageSize == 0 ? 100 : pageSize;
        return list.subList((page - 1) * p, Math.min(p * page, list.size()));
    }

    public int getPageCount(int totalSize, int pageSize) {
        if (totalSize == 0) {
            return 0;
        }
        int page = pageSize == 0 ? 100 : pageSize;
        return BigDecimal.valueOf(totalSize)
                .divide(BigDecimal.valueOf(page), 0, RoundingMode.UP)
                .intValue();
    }

}
