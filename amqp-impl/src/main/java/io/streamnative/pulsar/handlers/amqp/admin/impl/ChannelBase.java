package io.streamnative.pulsar.handlers.amqp.admin.impl;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.streamnative.pulsar.handlers.amqp.admin.model.ChannelBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.ConnectionBean;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;

@Slf4j
public class ChannelBase extends BaseResources{
    public CompletableFuture<ChannelBean> getChannelsListAsync(NamespaceName namespaceName) {

        Map<NamespaceName, Set<AmqpConnection>> connectionMap = aop().getAmqpBrokerService().getConnectionContainer()
                .getConnectionMap();
        String advertisedAddress = aop().getBrokerService().getPulsar().getAdvertisedAddress();
        ChannelBean channelBean = new ChannelBean();
        List<ChannelBean.ItemsBean> beanList = Lists.newArrayList();
        if (connectionMap.containsKey(namespaceName)) {
            Set<AmqpConnection> amqpConnections = connectionMap.get(namespaceName);
            amqpConnections.stream()
                    .map(amqpConnection -> amqpConnection.getChannels().values())
                    .flatMap(Collection::stream)
                    .map(amqpChannel -> {
                        ChannelBean.ItemsBean itemsBean = new ChannelBean.ItemsBean();
                        itemsBean.setName(amqpChannel.getConnection().getClientIp()+" -> " + advertisedAddress);
                        itemsBean.setState("running");
                        itemsBean.setConfirm(true);
                        itemsBean.setVhost(namespaceName.getLocalName());
                        itemsBean.setUser("");
                        itemsBean.setUser_who_performed_action("");
                        itemsBean.setNode("");
                        itemsBean.setPrefetch_count(amqpChannel.getCreditManager().getMessageCredit());
                        return itemsBean;
                    }).collect(Collectors.toCollection(() -> beanList));
        }
        channelBean.setItems(beanList);
        return CompletableFuture.completedFuture(channelBean);
    }
}
