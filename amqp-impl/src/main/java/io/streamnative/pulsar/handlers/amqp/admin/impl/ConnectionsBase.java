package io.streamnative.pulsar.handlers.amqp.admin.impl;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.streamnative.pulsar.handlers.amqp.admin.model.ConnectionBean;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;

@Slf4j
public class ConnectionsBase extends BaseResources {

    public CompletableFuture<ConnectionBean> getConnectionsListAsync(NamespaceName namespaceName) {

        Map<NamespaceName, Set<AmqpConnection>> connectionMap = aop().getAmqpBrokerService().getConnectionContainer()
                .getConnectionMap();
        String advertisedAddress = aop().getBrokerService().getPulsar().getAdvertisedAddress();
        ConnectionBean connectionBean = new ConnectionBean();
        List<ConnectionBean.ItemsBean> beanList = Lists.newArrayList();
        if (connectionMap.containsKey(namespaceName)) {
            Set<AmqpConnection> amqpConnections = connectionMap.get(namespaceName);
            amqpConnections.stream()
                    .map(amqpConnection -> {
                        ConnectionBean.ItemsBean itemsBean = new ConnectionBean.ItemsBean();

                        itemsBean.setAuth_mechanism("PLAIN");
                        itemsBean.setChannel_max(amqpConnection.getMaxChannels());
                        itemsBean.setChannels(amqpConnection.getChannels().size());
                        itemsBean.setConnected_at(amqpConnection.getConnectedAt());
                        itemsBean.setFrame_max(amqpConnection.getMaxFrameSize());
                        itemsBean.setHost(advertisedAddress);
                        itemsBean.setName(amqpConnection.getClientIp() + " -> " + advertisedAddress + ":5672");
                        itemsBean.setNode("");
                        itemsBean.setPort(5672);
                        itemsBean.setState("running");
                        itemsBean.setTimeout(amqpConnection.getHeartBeat());
                        itemsBean.setType("network");
                        itemsBean.setUser("");
                        itemsBean.setUser_provided_name("");
                        itemsBean.setVhost(namespaceName.getLocalName());
                        itemsBean.setUser_who_performed_action("");

                        return itemsBean;
                    }).collect(Collectors.toCollection(() -> beanList));
        }
        connectionBean.setItems(beanList);
        return CompletableFuture.completedFuture(connectionBean);
    }

}
