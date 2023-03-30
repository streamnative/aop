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
package io.streamnative.pulsar.handlers.amqp.admin;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.admin.impl.ExchangeBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.PublishParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.ExchangesList;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Exchange endpoints.
 */
@Slf4j
@Path("/exchanges")
@Produces(MediaType.APPLICATION_JSON)
public class Exchanges extends ExchangeBase {

    @GET
    public void getList(@Suspended final AsyncResponse response) {
        getExchangeListAsync()
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get exchange list for tenant {}", tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}")
    public void getListByVhost(@Suspended final AsyncResponse response,
                               @PathParam("vhost") String vhost,
                               @QueryParam("page") int page,
                               @QueryParam("page_size") int pageSize,
                               @QueryParam("name") String name,
                               @QueryParam("pagination") boolean pagination) {
        getExchangeListByNamespaceAsync(vhost)
                .thenAccept(itemsBeans -> {
                    int total = itemsBeans.size();
                    if (StringUtils.isNotBlank(name)) {
                        itemsBeans = itemsBeans.stream()
                                .filter(itemsBean -> itemsBean.getName().contains(name))
                                .collect(Collectors.toList());
                    }
                    ExchangesList exchangesList = new ExchangesList();
                    exchangesList.setPage(page);
                    exchangesList.setFiltered_count(itemsBeans.size());
                    exchangesList.setPage_size(pageSize);
                    exchangesList.setPage_count(getPageCount(itemsBeans.size(), pageSize));
                    exchangesList.setTotal_count(total);
                    exchangesList.setItems(getPageList(itemsBeans, page, pageSize));
                    exchangesList.setItem_count(exchangesList.getItems().size());
                    response.resume(exchangesList);
                })
                .exceptionally(t -> {
                    log.error("Failed to get exchange list by vhost {} for tenant {}", vhost, tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/{exchange}")
    public void getExchange(@Suspended final AsyncResponse response,
                            @PathParam("vhost") String vhost,
                            @PathParam("exchange") String exchange,
                            @QueryParam("msg_rates_age") int age,
                            @QueryParam("msg_rates_incr") int incr,
                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                getNamespaceName(vhost), PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getExchangeDetailAsync(vhost, exchange, age, incr))
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to get exchange {} for tenant {} belong to vhost {}",
                                exchange, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/{exchange}/bindings/source")
    public void getExchangeSource(@Suspended final AsyncResponse response,
                                  @PathParam("vhost") String vhost,
                                  @PathParam("exchange") String exchange,
                                  @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                getNamespaceName(vhost), PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(___ -> getExchangeSourceAsync(vhost, exchange))
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to get exchange {} for tenant {} belong to vhost {}",
                                exchange, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/{exchange}/bindings/destination")
    public void getExchangeDestination(@Suspended final AsyncResponse response,
                                       @PathParam("vhost") String vhost,
                                       @PathParam("exchange") String exchange) {
        response.resume(Lists.newArrayList());
    }

    @PUT
    @Path("/{vhost}/{exchange}")
    public void declareExchange(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("exchange") String exchange,
                                ExchangeDeclareParams params,
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> declareExchange(namespaceName, exchange, params))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare exchange {} for tenant {} belong to vhost {}",
                                exchange, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @DELETE
    @Path("/{vhost}/{exchange}")
    public void deleteExchange(@Suspended final AsyncResponse response,
                               @PathParam("vhost") String vhost,
                               @PathParam("exchange") String exchange,
                               @QueryParam("if-unused") boolean ifUnused,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> deleteExchange(namespaceName, exchange, ifUnused))
                .thenAccept(__ -> {
                    log.info("Success delete exchange {} in vhost {}, ifUnused is {}", exchange, vhost, ifUnused);
                    response.resume(Response.noContent().build());
                })
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to delete exchange {} for tenant {} belong to vhost {}",
                                exchange, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @POST
    @Path("/{vhost}/{exchange}/publish")
    public void publish(@Suspended final AsyncResponse response,
                        @PathParam("vhost") String vhost,
                        @PathParam("exchange") String exchange,
                        PublishParams params,
                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {

        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> asyncPublish(namespaceName, exchange, params))
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare exchange {} for tenant {} belong to vhost {}",
                                exchange, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }
}
