package io.streamnative.pulsar.handlers.amqp.admin;

import io.streamnative.pulsar.handlers.amqp.admin.impl.ConnectionsBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.ConnectionBean;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
@Path("/connections")
@Produces(MediaType.APPLICATION_JSON)
public class Connections extends ConnectionsBase {

    @GET
    public void getList(@Suspended final AsyncResponse response,
                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                        @QueryParam("page") int page,
                        @QueryParam("page_size") int pageSize,
                        @QueryParam("name") String name,
                        @QueryParam("pagination") boolean pagination) {
        NamespaceName namespaceName = getNamespaceName();
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, "__lookup__");
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getConnectionsListAsync(namespaceName))
                .thenAccept(connectionBean -> {
                    List<ConnectionBean.ItemsBean> itemsBeans = connectionBean.getItems();
                    int total = itemsBeans.size();
                    if (StringUtils.isNotBlank(name)) {
                        itemsBeans = itemsBeans.stream()
                                .filter(itemsBean -> itemsBean.getName().contains(name))
                                .collect(Collectors.toList());
                    }
                    connectionBean.setPage(page);
                    connectionBean.setFiltered_count(itemsBeans.size());
                    connectionBean.setPage_size(pageSize);
                    connectionBean.setPage_count(getPageCount(itemsBeans.size(), pageSize));
                    connectionBean.setTotal_count(total);
                    connectionBean.setItems(getPageList(itemsBeans, page, pageSize));
                    connectionBean.setItem_count(connectionBean.getItems().size());
                    response.resume(connectionBean);
                })
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to get connections belong to vhost {}", xVhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
