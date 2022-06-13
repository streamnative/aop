package io.streamnative.pulsar.handlers.amqp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

/**
 * ObjectMapper util.
 */
public class MapperUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> List<T> readListValue(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, new TypeReference<List<T>>(){});
    }

}
