package io.streamnative.pulsar.handlers.amqp.redirect;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;

public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    private RedirectService redirectService;

    public ServiceChannelInitializer(RedirectService redirectService) {
        this.redirectService = redirectService;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("frameEncoder",
                new AmqpEncoder());
        ch.pipeline().addLast("handler",
                new RedirectConnection(redirectService));
    }

}
