package io.streamnative.pulsar.handlers.amqp.redirect;

import com.google.common.collect.Maps;

import java.util.Map;

public class VHostBrokerDiscoverProvider {



    private Map<String, String> vhostBrokerMap = Maps.newConcurrentMap();

    public void getBroker(String vhost) {

    }

}
