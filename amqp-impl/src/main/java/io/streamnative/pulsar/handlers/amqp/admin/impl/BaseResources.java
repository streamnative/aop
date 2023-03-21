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
package io.streamnative.pulsar.handlers.amqp.admin.impl;

import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.ExchangeService;
import io.streamnative.pulsar.handlers.amqp.QueueContainer;
import io.streamnative.pulsar.handlers.amqp.QueueService;
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Base resources.
 */
@Slf4j
public class BaseResources {

    protected String tenant = "public";

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private AmqpProtocolHandler protocolHandler;

    private NamespaceService namespaceService;

    private NamespaceResources namespaceResources;

    private ExchangeService exchangeService;

    private ExchangeContainer exchangeContainer;

    private QueueService queueService;

    private QueueContainer queueContainer;

    protected AmqpProtocolHandler aop() {
        if (protocolHandler == null) {
            protocolHandler = (AmqpProtocolHandler) servletContext.getAttribute("aop");
        }
        return protocolHandler;
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

    protected CompletableFuture<List<VhostBean>> getVhostListAsync() {
        return namespaceResource().listNamespacesAsync(tenant)
                .thenApply(nsList -> {
                    List<VhostBean> vhostBeanList = new ArrayList<>();
                    nsList.forEach(ns -> {
                        VhostBean bean = new VhostBean();
                        bean.setName(ns);
                        vhostBeanList.add(bean);
                    });
                    return vhostBeanList;
                });
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
        return  pulsar.getLeaderElectionService().isLeader();
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
        } else {
            asyncResponse.resume(new RestException(realCause));
        }
    }

}
