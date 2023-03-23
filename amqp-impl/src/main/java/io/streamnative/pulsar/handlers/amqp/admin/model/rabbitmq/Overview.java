package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.AopVersion;
import io.streamnative.pulsar.handlers.amqp.admin.impl.BaseResources;
import io.streamnative.pulsar.handlers.amqp.admin.mock.MockData;
import java.util.Collection;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class Overview extends BaseResources {

    private static final ObjectMapper mapper = new ObjectMapper();

    @GET
    @Path("/overview")
    public Response overview(@QueryParam("lengths_age") Integer lengths_age,
                             @QueryParam("lengths_incr") Integer lengths_incr,
                             @QueryParam("msg_rates_age") Integer msg_rates_age,
                             @QueryParam("msg_rates_incr") Integer msg_rates_incr) throws JsonProcessingException {

        OverviewBean overviewBean = new OverviewBean();
        // ObjectTotalsBean
        overviewBean.setObject_totals(new OverviewBean.ObjectTotalsBean());
        // queueTotalsBean
        OverviewBean.QueueTotalsBean queueTotalsBean = new OverviewBean.QueueTotalsBean();
        OverviewBean.RateBean messagesDetailsBean =
                new OverviewBean.RateBean();
        messagesDetailsBean.setSamples(Lists.newArrayList(
                new OverviewBean.SamplesBean()));
        queueTotalsBean.setMessages_details(messagesDetailsBean);
        OverviewBean.RateBean detailsBean =
                new OverviewBean.RateBean();
        detailsBean.setSamples(Lists.newArrayList(
                new OverviewBean.SamplesBean()));
        queueTotalsBean.setMessages_ready_details(detailsBean);
        OverviewBean.RateBean unacknowledgedDetailsBean =
                new OverviewBean.RateBean();
        unacknowledgedDetailsBean.setSamples(Lists.newArrayList(
                new OverviewBean.SamplesBean()));
        queueTotalsBean.setMessages_unacknowledged_details(unacknowledgedDetailsBean);

        Collection<TopicStatsImpl> topicStats = aop().getBrokerService().getTopicStats().values();
        long count = topicStats.stream().map(TopicStatsImpl::getMsgInCounter)
                .reduce(Long::sum)
                .orElse(0L);
        Number ready = topicStats.stream()
                .map(topicStats1 -> {
                    SubscriptionStats subscriptionStats = topicStats1.getSubscriptions().get("AMQP_DEFAULT");
                    if (subscriptionStats == null) {
                        return 0;
                    }
                    return subscriptionStats.getMsgBacklog() - subscriptionStats.getUnackedMessages();
                }).reduce((number, number2) -> number.longValue() + number2.longValue())
                .orElse(0);
        Number unAck = topicStats.stream()
                .map(topicStats1 -> {
                    SubscriptionStats subscriptionStats = topicStats1.getSubscriptions().get("AMQP_DEFAULT");
                    if (subscriptionStats == null) {
                        return 0;
                    }
                    return subscriptionStats.getUnackedMessages();
                }).reduce((number, number2) -> number.longValue() + number2.longValue())
                .orElse(0);
        queueTotalsBean.setMessages(count);
        queueTotalsBean.setMessages_ready(ready.longValue());
        queueTotalsBean.setMessages_unacknowledged(unAck.longValue());

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

        return Response.ok(overviewBean).build();
    }

    @GET
    @Path("/nodes")
    public Response nodes() throws JsonProcessingException {

        return Response.ok(MockData.nodes).build();
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

}
