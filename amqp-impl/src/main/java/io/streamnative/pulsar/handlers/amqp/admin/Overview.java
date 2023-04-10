package io.streamnative.pulsar.handlers.amqp.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.AopVersion;
import io.streamnative.pulsar.handlers.amqp.admin.impl.BaseResources;
import io.streamnative.pulsar.handlers.amqp.admin.mock.MockData;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueUnBindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.OverviewBean;
import io.streamnative.pulsar.handlers.amqp.admin.prometheus.PrometheusAdmin;
import io.streamnative.pulsar.handlers.amqp.admin.prometheus.QueueRangeMetrics;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class Overview extends BaseResources {

    private static final ObjectMapper mapper = new ObjectMapper();

    @POST
    @Path("/queueBindExchange/{vhost}/e/{exchange}/q/{queue}")
    public void queueBindExchange(@Suspended final AsyncResponse response,
                          @PathParam("vhost") String vhost,
                          @PathParam("exchange") String exchange,
                          @PathParam("queue") String queue,
                          BindingParams params,
                          @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentQueue.TOPIC_PREFIX + queue);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> queueBindExchange(namespaceName, exchange, queue, params))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to bind exchange {} to queue {} with key {} in vhost {}",
                                queue, exchange, params.getRoutingKey(), vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @DELETE
    @Path("/queueUnBindExchange/{vhost}/e/{exchange}/q/{queue}/unbind")
    public void queueUnBindExchange(@Suspended final AsyncResponse response,
                           @PathParam("vhost") String vhost,
                           @PathParam("exchange") String exchange,
                           @PathParam("queue") String queue,
                           QueueUnBindingParams params,
                           @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentQueue.TOPIC_PREFIX + queue);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> queueUnBindExchange(namespaceName, exchange, queue, params.getProperties_key()))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to unbind exchange {} to queue {} with key {} in vhost {}",
                                queue, exchange, params.getProperties_key(), vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/overview")
    public void overview(@Suspended final AsyncResponse response,
                         @QueryParam("lengths_age") @DefaultValue("60") Integer lengths_age,
                         @QueryParam("lengths_incr") @DefaultValue("5") Integer lengths_incr,
                         @QueryParam("msg_rates_age") Integer msg_rates_age,
                         @QueryParam("msg_rates_incr") Integer msg_rates_incr) throws JsonProcessingException {

        NamespaceName namespaceName = getNamespaceName();
        PrometheusAdmin prometheusAdmin = prometheusAdmin();
        CompletableFuture<QueueRangeMetrics> queueDetailMetrics;
        if (prometheusAdmin.isConfig()) {
            queueDetailMetrics =
                    prometheusAdmin.queryRangeNamespaceMetrics(
                            namespaceName.getTenant() + "/" + namespaceName.getLocalName(), lengths_age, lengths_incr);
        } else {
            queueDetailMetrics = CompletableFuture.completedFuture(null);
        }
        OverviewBean overviewBean = new OverviewBean();
        // TODO ObjectTotalsBean
        // overviewBean.setObject_totals(new OverviewBean.ObjectTotalsBean());
        OverviewBean.QueueTotalsBean queueTotalsBean = new OverviewBean.QueueTotalsBean();
        queueTotalsBean.setMessages_details(new OverviewBean.RateBean());
        queueTotalsBean.setMessages_ready_details(new OverviewBean.RateBean());
        queueTotalsBean.setMessages_unacknowledged_details(new OverviewBean.RateBean());

        overviewBean.setQueue_totals(queueTotalsBean);
        // rates_mode
        overviewBean.setRates_mode("basic");
        // messageStatsBean
        OverviewBean.MessageStatsBean messageStatsBean = new OverviewBean.MessageStatsBean();
        messageStatsBean.setAck_details(new OverviewBean.RateBean());
        messageStatsBean.setConfirm_details(new OverviewBean.RateBean());
        messageStatsBean.setDeliver_details(new OverviewBean.RateBean());
        messageStatsBean.setDeliver_no_ack_details(new OverviewBean.RateBean());
        messageStatsBean.setDeliver_get_details(new OverviewBean.RateBean());
        messageStatsBean.setDisk_reads_details(new OverviewBean.RateBean());
        messageStatsBean.setDisk_writes_details(new OverviewBean.RateBean());
        messageStatsBean.setGet_details(new OverviewBean.RateBean());
        messageStatsBean.setGet_no_ack_details(new OverviewBean.RateBean());
        messageStatsBean.setPublish_details(new OverviewBean.RateBean());
        messageStatsBean.setRedeliver_details(new OverviewBean.RateBean());
        messageStatsBean.setReturn_unroutable_details(new OverviewBean.RateBean());
        overviewBean.setMessage_stats(messageStatsBean);

        overviewBean.setExchange_types(mapper.readValue(MockData.exchangesType,
                new TypeReference<List<OverviewBean.ExchangeTypesBean>>() {
                }));

        overviewBean.setSample_retention_policies(mapper.readValue(MockData.sample_retention_policies,
                new TypeReference<OverviewBean.SampleRetentionPoliciesBean>() {
                }));
        OverviewBean.ChurnRatesBean churnRatesBean = new OverviewBean.ChurnRatesBean();
        churnRatesBean.setChannel_closed_details(new OverviewBean.RateBean());
        churnRatesBean.setChannel_created_details(new OverviewBean.RateBean());
        churnRatesBean.setConnection_closed_details(new OverviewBean.RateBean());
        churnRatesBean.setConnection_created_details(new OverviewBean.RateBean());
        churnRatesBean.setQueue_created_details(new OverviewBean.RateBean());
        churnRatesBean.setQueue_deleted_details(new OverviewBean.RateBean());
        churnRatesBean.setQueue_declared_details(new OverviewBean.RateBean());
        overviewBean.setChurn_rates(churnRatesBean);
        overviewBean.setCluster_name(aop().getBrokerService().pulsar().getConfiguration().getClusterName());
        overviewBean.setRabbitmq_version("AMQP on Pulsar " + AopVersion.getVersion());
        overviewBean.setErlang_version(Runtime.version().toString());
        OverviewBean.ContextsBean contextsBean = new OverviewBean.ContextsBean();
        contextsBean.setIp("");
        contextsBean.setNode("");
        contextsBean.setPath("");
        contextsBean.setSsl_opts(new OverviewBean.ContextsBean.SslOptsBean());
        contextsBean.setPort("");
        overviewBean.setContexts(Lists.newArrayList(contextsBean));
        overviewBean.setListeners(Lists.newArrayList(new OverviewBean.ListenersBean()));
        overviewBean.setNode("");

        queueDetailMetrics.thenAccept(metrics -> {
            if (CollectionUtils.isNotEmpty(metrics.getTotal())) {
                overviewBean.getQueue_totals().setMessages((long) metrics.getTotal().get(0).getSample());
            }
            overviewBean.getQueue_totals().getMessages_details().setSamples(metrics.getTotal());

            if (CollectionUtils.isNotEmpty(metrics.getReady())) {
                overviewBean.getQueue_totals().setMessages_ready((long) metrics.getReady().get(0).getSample());
            }
            overviewBean.getQueue_totals().getMessages_ready_details().setSamples(metrics.getReady());

            if (CollectionUtils.isNotEmpty(metrics.getUnAck())) {
                overviewBean.getQueue_totals().setMessages_unacknowledged((long) metrics.getUnAck().get(0).getSample());
            }
            overviewBean.getQueue_totals().getMessages_unacknowledged_details().setSamples(metrics.getUnAck());

            overviewBean.getMessage_stats().getPublish_details().setSamples(metrics.getPublish());

            overviewBean.getMessage_stats().setPublish((long) metrics.getPublishValue());
            overviewBean.getMessage_stats().getPublish_details().setRate(metrics.getPublishValue());

            overviewBean.getMessage_stats().setConfirm((long) metrics.getPublishValue());
            overviewBean.getMessage_stats().getConfirm_details().setRate(metrics.getPublishValue());

            // overviewBean.getMessage_stats().getAck_details().setSamples(metrics.getAck());
            if (CollectionUtils.isNotEmpty(metrics.getAck())) {
                overviewBean.getMessage_stats().setAck((long) metrics.getAck().get(0).getSample());
                overviewBean.getMessage_stats().getAck_details().setRate(metrics.getAck().get(0).getSample());
            }

            overviewBean.getMessage_stats().setDeliver((long) metrics.getDeliverValue());
            overviewBean.getMessage_stats().getDeliver_details().setRate((long) metrics.getDeliverValue());
            overviewBean.getMessage_stats().getDeliver_get_details().setRate((long) metrics.getDeliverValue());
            overviewBean.getMessage_stats().setDeliver_get((long) metrics.getDeliverValue());

            overviewBean.getMessage_stats().getDeliver_get_details().setSamples(metrics.getDeliver());
            overviewBean.getMessage_stats().getDeliver_details().setSamples(metrics.getDeliver());
            overviewBean.getMessage_stats().getConfirm_details().setSamples(metrics.getDeliver());

            response.resume(overviewBean);
        });
    }

    @GET
    @Path("/nodes")
    public Response nodes() {

        return Response.ok("{}").build();
    }

    @GET
    @Path("/extensions")
    public Response extensions() throws JsonProcessingException {
        return Response.ok(MockData.extensions).build();
    }

    @GET
    @Path("/auth")
    public Response auth() throws JsonProcessingException {
        return Response.ok("{}").build();
    }
    @GET
    @Path("/permissions")
    public Response permissions() throws JsonProcessingException {
        return Response.ok("[]").build();
    }

}
