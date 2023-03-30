package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.SamplesBean;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.utils.HttpUtil;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class PrometheusAdmin {

    private final String url;
    private final String clusterName;

    public PrometheusAdmin(String url, String clusterName) {
        this.url = url;
        this.clusterName = clusterName;
    }

    public boolean isConfig() {
        return url != null;
    }

    public static void main(String[] args) throws Exception {

        PrometheusAdmin admin = new PrometheusAdmin("http://10.70.20.193:9090/", "pulsar-cluster-test");

        admin.queryRangeQueueDetailMetrics("amqp/default","basic.customs.goods.notify.queue", 7200, 120).thenAccept(System.out::println);
    }

    public CompletableFuture<Map<String, QueueListMetrics>> queryQueueListMetrics(String namespace) {
        String topic = "topic=~\"persistent://" + namespace + "/__amqp_queue__.*?\"";
        // ready
        CompletableFuture<Map<String, Double>> readyFuture =
                queryMetrics(Lists.newArrayList(topic, "subscription=\"AMQP_DEFAULT\""), "pulsar_subscription_back_log")
                        .thenApply(MetricsResponse::getTopicValueMap);
        // unAck
        CompletableFuture<Map<String, Double>> unAckFuture =
                queryMetrics(Lists.newArrayList(topic), "pulsar_subscription_unacked_messages")
                        .thenApply(MetricsResponse::getTopicValueMap);
        // total
        CompletableFuture<Map<String, Double>> totalFuture =
                queryMetrics(Lists.newArrayList(topic), "pulsar_in_messages_total")
                        .thenApply(MetricsResponse::getTopicValueMap);

        // rate
        // incoming
        CompletableFuture<Map<String, Double>> pulsarRateIn =
                queryMetrics(Lists.newArrayList(topic), "pulsar_rate_in")
                        .thenApply(MetricsResponse::getTopicValueMap);
        // deliver
        CompletableFuture<Map<String, Double>> pulsarRateOut =
                queryMetrics(Lists.newArrayList(topic), "pulsar_rate_out")
                        .thenApply(MetricsResponse::getTopicValueMap);
        // ack
        CompletableFuture<Map<String, Double>> ackRateFuture =
                queryMetrics(Lists.newArrayList(topic), "pulsar_subscription_msg_ack_rate")
                        .thenApply(MetricsResponse::getTopicValueMap);

        return FutureUtil.waitForAll(
                        Lists.newArrayList(readyFuture, unAckFuture, totalFuture, pulsarRateIn, pulsarRateOut,
                                ackRateFuture))
                .thenApply(__ -> {
                    Map<String, QueueListMetrics> metrics = Maps.newHashMap();
                    try {
                        Map<String, Double> readyMap = readyFuture.get();
                        Map<String, Double> unAckMap = unAckFuture.get();
                        Map<String, Double> totalMap = totalFuture.get();
                        Map<String, Double> inRate = pulsarRateIn.get();
                        Map<String, Double> outRate = pulsarRateOut.get();
                        Map<String, Double> ackRate = ackRateFuture.get();
                        unAckMap.forEach((key, value) -> {
                            QueueListMetrics queueListMetrics = new QueueListMetrics();
                            queueListMetrics.setReady(value.longValue());
                            queueListMetrics.setUnAck(readyMap.getOrDefault(key, 0.0).longValue());
                            queueListMetrics.setTotal(totalMap.getOrDefault(key, 0.0).longValue());

                            queueListMetrics.setIncomingRate(inRate.getOrDefault(key, 0.0));
                            queueListMetrics.setDeliverRate(outRate.getOrDefault(key, 0.0));
                            queueListMetrics.setAckRate(ackRate.getOrDefault(key, 0.0));
                            metrics.put(key, queueListMetrics);
                        });
                    } catch (Exception ex) {
                        log.error("", ex);
                    }
                    return metrics;
                });
    }

    public CompletableFuture<QueueRangeMetrics> queryRangeNamespaceMetrics(String namespace, long age, long incr) {
        String topic = "topic=~\"persistent://" + namespace + "/__amqp_queue__.*?\"";
        // ready
        CompletableFuture<List<SamplesBean>> readyFuture =
                queryRangeMetrics(Lists.newArrayList(topic, "subscription=\"AMQP_DEFAULT\""),
                        "sum(pulsar_subscription_back_log", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);

        // unAck
        CompletableFuture<List<SamplesBean>> unAckFuture =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_subscription_unacked_messages", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);
        // total
        CompletableFuture<List<SamplesBean>> totalFuture =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_in_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);
        // rate
        // incoming
        CompletableFuture<List<SamplesBean>> pulsarRateIn =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_in_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);
        // deliver
        CompletableFuture<List<SamplesBean>> pulsarRateOut =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_out_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);

        // incoming
        CompletableFuture<List<SamplesBean>> pulsarIn =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_rate_in", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);
        // deliver
        CompletableFuture<List<SamplesBean>> pulsarOut =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_rate_out", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);

        // ack
        CompletableFuture<List<SamplesBean>> ackRateFuture =
                queryRangeMetrics(Lists.newArrayList(topic), "sum(pulsar_subscription_msg_ack_rate", age, incr)
                        .thenApply(MetricsRangeResponse::getValueMap);

        return FutureUtil.waitForAll(
                        Lists.newArrayList(readyFuture, unAckFuture, totalFuture, pulsarRateIn, pulsarRateOut,
                                ackRateFuture, pulsarIn, pulsarOut))
                .thenApply(unused -> {
                    QueueRangeMetrics metrics = new QueueRangeMetrics();
                    try {
                        List<SamplesBean> unAck = unAckFuture.get();
                        List<SamplesBean> ready = readyFuture.get();
                        List<SamplesBean> total = totalFuture.get();
                        List<SamplesBean> inRate = pulsarRateIn.get();
                        List<SamplesBean> outRate = pulsarRateOut.get();
                        List<SamplesBean> ackRate = ackRateFuture.get();
                        List<SamplesBean> in = pulsarIn.get();
                        List<SamplesBean> out = pulsarOut.get();
                        metrics.setUnAck(unAck);
                        metrics.setReady(ready);
                        metrics.setTotal(total);
                        metrics.setAck(ackRate);
                        metrics.setDeliver(outRate);
                        metrics.setPublish(inRate);
                        if (CollectionUtils.isNotEmpty(pulsarIn.get())) {
                            metrics.setPublishValue(in.get(0).getSample());
                        }
                        if (CollectionUtils.isNotEmpty(pulsarOut.get())) {
                            metrics.setDeliverValue(out.get(0).getSample());
                        }
                    } catch (Exception ex) {
                        log.error("", ex);
                    }
                    return metrics;
                });
    }

    public CompletableFuture<Map<String, QueueRangeMetrics>> queryRangeQueueDetailMetrics(String namespace,
                                                                                          String queue, long age,
                                                                                          long incr) {

        String topic = "topic=\"persistent://" + namespace + "/__amqp_queue__" + queue + "\"";

        // ready
        CompletableFuture<Map<String, List<SamplesBean>>> readyFuture =
                queryRangeMetrics(Lists.newArrayList(topic, "subscription=\"AMQP_DEFAULT\""),
                        "pulsar_subscription_back_log", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);
        // unAck
        CompletableFuture<Map<String, List<SamplesBean>>> unAckFuture =
                queryRangeMetrics(Lists.newArrayList(topic), "pulsar_subscription_unacked_messages", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);
        // total
        CompletableFuture<Map<String, List<SamplesBean>>> totalFuture =
                queryRangeMetrics(Lists.newArrayList(topic), "pulsar_in_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);
        // rate
        // incoming
        CompletableFuture<Map<String, List<SamplesBean>>> pulsarRateIn =
                queryRangeMetrics(Lists.newArrayList(topic), "pulsar_in_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);
        // deliver
        CompletableFuture<Map<String, List<SamplesBean>>> pulsarRateOut =
                queryRangeMetrics(Lists.newArrayList(topic), "pulsar_out_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);
        // ack
        CompletableFuture<Map<String, List<SamplesBean>>> ackRateFuture =
                queryRangeMetrics(Lists.newArrayList(topic), "pulsar_subscription_msg_ack_rate", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);

        return FutureUtil.waitForAll(
                        Lists.newArrayList(readyFuture, unAckFuture, totalFuture, pulsarRateIn, pulsarRateOut,
                                ackRateFuture))
                .thenApply(unused -> {
                    Map<String, QueueRangeMetrics> metrics = Maps.newHashMap();
                    try {
                        Map<String, List<SamplesBean>> unAckMap = unAckFuture.get();
                        Map<String, List<SamplesBean>> readyMap = readyFuture.get();
                        Map<String, List<SamplesBean>> totalMap = totalFuture.get();
                        Map<String, List<SamplesBean>> inRate = pulsarRateIn.get();
                        Map<String, List<SamplesBean>> outRate = pulsarRateOut.get();
                        Map<String, List<SamplesBean>> ackRate = ackRateFuture.get();
                        unAckMap.forEach((key, value) -> {
                            QueueRangeMetrics rangeMetrics = new QueueRangeMetrics();
                            rangeMetrics.setUnAck(value);
                            rangeMetrics.setReady(readyMap.get(key));
                            rangeMetrics.setTotal(totalMap.get(key));
                            rangeMetrics.setAck(ackRate.get(key));
                            rangeMetrics.setDeliver(outRate.get(key));
                            rangeMetrics.setPublish(inRate.get(key));
                            rangeMetrics.setTopic(key);
                            metrics.put(key, rangeMetrics);
                        });
                    } catch (Exception ignore) {
                        //
                    }
                    return metrics;
                }).exceptionally(throwable -> {
                    log.error("", throwable);
                    return null;
                });
    }

    public CompletableFuture<Map<String, ExchangeRangeMetrics>> queryRangeExchangeDetailMetrics(String namespace,
                                                                                                String topic, long age,
                                                                                                long incr) {
        CompletableFuture<Map<String, List<SamplesBean>>> pulsarRateIn =
                queryRangeMetrics(
                        Lists.newArrayList("topic=\"persistent://" + namespace + "/__amqp_exchange__" + topic + "\""),
                        "pulsar_in_messages_total", age, incr)
                        .thenApply(MetricsRangeResponse::getTopicValueMap);

        return FutureUtil.waitForAll(Lists.newArrayList(pulsarRateIn))
                .thenApply(__ -> {
                    Map<String, ExchangeRangeMetrics> metrics = Maps.newHashMap();
                    try {
                        Map<String, List<SamplesBean>> inRate = pulsarRateIn.get();
                        inRate.forEach((key, value) -> {
                            ExchangeRangeMetrics rangeMetrics = new ExchangeRangeMetrics();
                            rangeMetrics.setIn(value);
                            rangeMetrics.setTopic(key);
                            metrics.put(key, rangeMetrics);
                        });
                    } catch (Exception ignore) {
                        //
                    }
                    return metrics;
                });
    }

    public CompletableFuture<Map<String, ExchangeListMetrics>> queryAllExchangeMetrics(String namespace) {
        String topic = "topic=~\"persistent://" + namespace + "/__amqp_exchange__.*?\"";
        CompletableFuture<Map<String, Double>> pulsarRateIn =
                queryMetrics(Lists.newArrayList(topic), "pulsar_rate_in")
                        .thenApply(MetricsResponse::getTopicValueMap);
        return FutureUtil.waitForAll(
                        Lists.newArrayList(pulsarRateIn))
                .thenApply(__ -> {
                    Map<String, ExchangeListMetrics> metrics = Maps.newHashMap();
                    try {
                        Map<String, Double> inRate = pulsarRateIn.get();
                        inRate.forEach((key, value) -> {
                            ExchangeListMetrics exchangeListMetrics = new ExchangeListMetrics();
                            exchangeListMetrics.setTopicName(key);
                            exchangeListMetrics.setInRate(value);
                            metrics.put(key, exchangeListMetrics);
                        });
                    } catch (Exception ignore) {
                        //
                    }
                    return metrics;
                });
    }

    public CompletableFuture<MetricsRangeResponse> queryRangeMetrics(List<String> list, String metricsName, long age,
                                                                     long incr) {
        if (this.url == null) {
            throw new AoPServiceRuntimeException("Prometheus url not configured.");
        }
        list.add("cluster=\"" + clusterName + "\"");
        String metric = String.join(",", list);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        String end = seconds + ".000";
        String start = (seconds - age) + ".000";
        String flag = metricsName.contains("(") ? ")" : "";
        String url = this.url + "/api/v1/query_range?"
                + "query="
                + URLEncoder.encode(metricsName
                + "{"
                + metric
                + "}"
                + flag, StandardCharsets.UTF_8)
                + "&" + "start" + "="
                + start
                + "&" + "end" + "="
                + end
                + "&" + "step" + "="
                + incr;
        return HttpUtil.getAsync(url, MetricsRangeResponse.class);
    }

    public CompletableFuture<MetricsResponse> queryMetrics(List<String> list, String metricsName) {
        if (this.url == null) {
            throw new AoPServiceRuntimeException("Prometheus url not configured.");
        }
        list.add("cluster=\"" + clusterName + "\"");
        String metric = String.join(",", list);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        String start = seconds + ".000";
        String url = this.url + "/api/v1/query?"
                + "query="
                + URLEncoder.encode(metricsName
                + "{"
                + metric + "}", StandardCharsets.UTF_8)
                + "&" + "time" + "="
                + start;
        return HttpUtil.getAsync(url, MetricsResponse.class);
    }

}
