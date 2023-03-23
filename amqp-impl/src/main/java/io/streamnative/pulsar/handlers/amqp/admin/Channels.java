package io.streamnative.pulsar.handlers.amqp.admin;

import io.streamnative.pulsar.handlers.amqp.admin.impl.ChannelBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.ChannelBean;
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
@Path("/channels")
@Produces(MediaType.APPLICATION_JSON)
public class Channels extends ChannelBase {

    @GET
    public void getList(@Suspended final AsyncResponse response,
                        @QueryParam("authoritative")
                        @DefaultValue("false") boolean authoritative,
                        @QueryParam("page") int page,
                        @QueryParam("page_size") int pageSize,
                        @QueryParam("name") String name,
                        @QueryParam("pagination") boolean pagination) {
        NamespaceName namespaceName = getNamespaceName();
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, "__lookup__");
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getChannelsListAsync(namespaceName))
                .thenAccept(channelBean -> {
                    List<ChannelBean.ItemsBean> itemsBeans = channelBean.getItems();
                    int total = itemsBeans.size();
                    if (StringUtils.isNotBlank(name)) {
                        itemsBeans = itemsBeans.stream()
                                .filter(itemsBean -> itemsBean.getName().contains(name))
                                .collect(Collectors.toList());
                    }
                    channelBean.setPage(page);
                    channelBean.setFiltered_count(itemsBeans.size());
                    channelBean.setPage_size(pageSize);
                    channelBean.setPage_count(getPageCount(itemsBeans.size(), pageSize));
                    channelBean.setTotal_count(total);
                    channelBean.setItems(getPageList(itemsBeans, page, pageSize));
                    channelBean.setItem_count(channelBean.getItems().size());
                    response.resume(channelBean);
                })
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to get channels belong to vhost {}", xVhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }
}
