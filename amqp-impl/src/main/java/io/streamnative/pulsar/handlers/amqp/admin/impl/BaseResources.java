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
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Base resources.
 */
public class BaseResources {

    protected String tenant = "public";

    @Context
    protected ServletContext servletContext;

    private AmqpProtocolHandler protocolHandler;

    private NamespaceService namespaceService;

    private NamespaceResources namespaceResources;

    private ExchangeService exchangeService;

    private ExchangeContainer exchangeContainer;

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

    protected ExchangeContainer exchangeContainer() {
        if (exchangeContainer == null) {
            exchangeContainer = aop().getAmqpBrokerService().getExchangeContainer();
        }
        return exchangeContainer;
    }

    protected ExchangeService exchangeService() {
        if (exchangeService == null) {
            exchangeService = aop().getAmqpBrokerService().getExchangeService();
        }
        return exchangeService;
    }

    protected static void resumeAsyncResponseExceptionally(AsyncResponse asyncResponse, Throwable exception) {
        Throwable realCause = FutureUtil.unwrapCompletionException(exception);
        if (realCause instanceof WebApplicationException) {
            asyncResponse.resume(realCause);
        } else {
            asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, realCause.getMessage()));
        }
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

}
