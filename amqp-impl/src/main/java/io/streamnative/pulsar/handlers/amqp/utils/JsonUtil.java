/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JsonUtils.
 */
public class JsonUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String toString(Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    public static <T> T parseObject(String jsonStr) throws IOException {
        return objectMapper.readValue(jsonStr, new TypeReference<T>() {
        });
    }

    public static <T> T parseObject(String jsonStr, Class<T> clazz) throws IOException {
        return objectMapper.readValue(jsonStr, clazz);
    }
    public static <T> T parseObject(String jsonStr, TypeReference<T> tTypeReference) throws IOException {
        return objectMapper.readValue(jsonStr, tTypeReference);
    }

    public static <T> List<T> parseObjectList(String json, Class<T> obj) throws JsonProcessingException {
        JavaType javaType = objectMapper.getTypeFactory().constructParametricType(ArrayList.class, obj);
        return objectMapper.readValue(json, javaType);
    }

    public static JsonNode readTree(String jsonStr) throws IOException {
        return objectMapper.readTree(jsonStr);
    }

    public static Map<String, Object> toMap(Object object) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(object);
        return objectMapper.readValue(json, Map.class);
    }

}
