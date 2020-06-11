package io.streamnative.pulsar.handlers.amqp.test;

import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

@Slf4j
public class ConfigNameTest {

    private static final String CATEGORY_NAME_PRE = "CATEGORY_AMQP";
    private static final String FIELD_NAME_PRE = "amqp";

    /**
     * This test is used to check config name
     */
    @Test
    public void amqpServiceConfigurationTest() {

        Class<AmqpServiceConfiguration> amqpServiceConfigurationClass = AmqpServiceConfiguration.class;
        checkConfigName(amqpServiceConfigurationClass);

        Class<ProxyConfiguration> proxyConfigurationClass = ProxyConfiguration.class;
        checkConfigName(proxyConfigurationClass);
    }

    private void checkConfigName(Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            boolean valid =
                    field.getName().startsWith(CATEGORY_NAME_PRE)
                            || field.getName().startsWith(FIELD_NAME_PRE);
            Assert.assertTrue(
                    valid, "The config name `" + field.getName() + "` should start with `"
                            + CATEGORY_NAME_PRE + "` or `" + FIELD_NAME_PRE + "`");
        }

    }

}
