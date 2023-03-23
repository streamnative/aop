package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.streamnative.pulsar.handlers.amqp.utils.JsonUtil;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/whoami")
@Produces(MediaType.APPLICATION_JSON)
public class Whomi {

    @GET
    public Response whomi() throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("name", "root");
        map.put("tags", "administrator");
        String str = JsonUtil.toString(map);
        return Response.ok(str).build();
    }
}
