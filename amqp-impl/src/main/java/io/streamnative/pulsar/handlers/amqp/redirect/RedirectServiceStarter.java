package io.streamnative.pulsar.handlers.amqp.redirect;

public class RedirectServiceStarter {

    public RedirectServiceStarter(String[] args) throws Exception {
        RedirectConfiguration redirectConfig = new RedirectConfiguration();
        RedirectService redirectService = new RedirectService(redirectConfig);
        redirectService.start();

        WebServer webServer = new WebServer(redirectConfig, null);
        webServer.start();
    }

    public static void main(String[] args) throws Exception {
        new RedirectServiceStarter(args);
    }

}
