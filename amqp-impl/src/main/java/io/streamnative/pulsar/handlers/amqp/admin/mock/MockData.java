package io.streamnative.pulsar.handlers.amqp.admin.mock;

public class MockData {

    public static String exchangesType = "[\n"
            + "  {\n"
            + "    \"name\": \"direct\",\n"
            + "    \"description\": \"AMQP direct exchange, as per the AMQP specification\",\n"
            + "    \"enabled\": true\n"
            + "  },\n"
            + "  {\n"
            + "    \"name\": \"x-delayed-message\",\n"
            + "    \"description\": \"Delayed Message Exchange.\",\n"
            + "    \"enabled\": true\n"
            + "  },\n"
            + "  {\n"
            + "    \"name\": \"fanout\",\n"
            + "    \"description\": \"AMQP fanout exchange, as per the AMQP specification\",\n"
            + "    \"enabled\": true\n"
            + "  },\n"
            + "  {\n"
            + "    \"name\": \"headers\",\n"
            + "    \"description\": \"AMQP headers exchange, as per the AMQP specification\",\n"
            + "    \"enabled\": true\n"
            + "  },\n"
            + "  {\n"
            + "    \"name\": \"topic\",\n"
            + "    \"description\": \"AMQP topic exchange, as per the AMQP specification\",\n"
            + "    \"enabled\": true\n"
            + "  }\n"
            + "]";

    public static final String sample_retention_policies= "{\n"
            + "  \"global\": [\n"
            + "    600,\n"
            + "    3600,\n"
            + "    28800,\n"
            + "    86400\n"
            + "  ],\n"
            + "  \"basic\": [\n"
            + "    600,\n"
            + "    3600\n"
            + "  ],\n"
            + "  \"detailed\": [\n"
            + "    600\n"
            + "  ]\n"
            + "}";

    public static final String extensions = "[{\"javascript\":\"dispatcher.js\"},[],{\"javascript\":\"top.js\"}]";

}
