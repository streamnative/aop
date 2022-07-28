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
package io.streamnative.pulsar.handlers.amqp;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.test.HttpUtil;
import com.rabbitmq.client.test.JsonUtil;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeBean;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Admin API test.
 */
public class AdminTest extends AmqpTestBase{

    @Test(timeOut = 1000 * 30)
    public void listTest() throws Exception {
        Connection connection = getConnection("vhost1", false);
        Channel channel = connection.createChannel();
        Set<String> vhost1Queues = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String ex = randExName();
            vhost1Queues.add(ex);
            channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true);
        }
        String ex1 = vhost1Queues.iterator().next();

        Connection connection2 = getConnection("vhost2", false);
        Channel channel2 = connection2.createChannel();
        Set<String> vhost2Queues = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String ex = randExName();
            vhost2Queues.add(ex);
            channel2.exchangeDeclare(ex, BuiltinExchangeType.FANOUT, true);
        }

        List<ExchangeBean> exchangeBeans = exchangeList();
        for (ExchangeBean bean : exchangeBeans) {
            if (vhost1Queues.remove(bean.getName())) {
                Assert.assertEquals(bean.getType(), "direct");
            }
            if (vhost2Queues.remove(bean.getName())) {
                Assert.assertEquals(bean.getType(), "fanout");
            }
        }
        Assert.assertEquals(vhost1Queues.size(), 0);
        Assert.assertEquals(vhost2Queues.size(), 0);

        List<ExchangeBean> exchangeBeans1 = exchangeListByVhost("vhost1");
        Assert.assertTrue(exchangeBeans1.size() > 0);
        for (ExchangeBean bean : exchangeBeans1) {
            Assert.assertEquals(bean.getVhost(), "vhost1");
        }

        ExchangeBean bean = exchangeByName("vhost1", ex1);
        Assert.assertEquals(bean.getName(), ex1);
    }

    @Test
    public void declareAndDeleteTest() throws IOException {
        String vhost = "vhost3";
        String ex1 = randExName();
        exchangeDeclare(vhost, ex1, BuiltinExchangeType.DIRECT.getType());
        String ex2 = randExName();
        exchangeDeclare(vhost, ex2, BuiltinExchangeType.TOPIC.getType());
        String ex3 = randExName();
        exchangeDeclare(vhost, ex3, BuiltinExchangeType.FANOUT.getType());
        String ex4 = randExName();
        exchangeDeclare(vhost, ex4, BuiltinExchangeType.HEADERS.getType());

        List<ExchangeBean> beans = exchangeListByVhost(vhost);
        Assert.assertEquals(beans.size(), 4);
        for (ExchangeBean bean : beans) {
            if (bean.getName().equals(ex1)) {
                Assert.assertEquals(bean.getType(), BuiltinExchangeType.DIRECT.getType().toLowerCase());
            } else if (bean.getName().equals(ex2)) {
                Assert.assertEquals(bean.getType(), BuiltinExchangeType.TOPIC.getType().toLowerCase());
            } else if (bean.getName().equals(ex3)) {
                Assert.assertEquals(bean.getType(), BuiltinExchangeType.FANOUT.getType().toLowerCase());
            } else if (bean.getName().equals(ex4)) {
                Assert.assertEquals(bean.getType(), BuiltinExchangeType.HEADERS.getType().toLowerCase());
            }
        }

        exchangeDelete(vhost, ex1);
        exchangeDelete(vhost, ex2);
        exchangeDelete(vhost, ex3);
        beans = exchangeListByVhost(vhost);
        Assert.assertEquals(beans.size(), 1);
        Assert.assertEquals(beans.get(0).getName(), ex4);
        exchangeDelete(vhost, ex4);
    }

    private void exchangeDeclare(String vhost, String exchange, String type) throws IOException {
        Map<String, Object> declareParams = new HashMap<>();
        declareParams.put("type", type);
        declareParams.put("auto_delete", true);
        declareParams.put("durable", true);
        declareParams.put("internal", false);
        HttpUtil.put(api("exchanges/" + vhost + "/" + exchange), declareParams);
    }

    private void exchangeDelete(String vhost, String exchange) throws IOException {
        HttpUtil.delete(api("exchanges/" + vhost + "/" + exchange));
    }

    private List<ExchangeBean> exchangeList() throws IOException {
        String exchangeJson = HttpUtil.get(api("exchanges"));
        return JsonUtil.parseObjectList(exchangeJson, ExchangeBean.class);
    }

    private List<ExchangeBean> exchangeListByVhost(String vhost) throws IOException {
        String exchangeJson = HttpUtil.get(api("exchanges/" + vhost));
        return JsonUtil.parseObjectList(exchangeJson, ExchangeBean.class);
    }

    private ExchangeBean exchangeByName(String vhost, String exchange) throws IOException {
        String exchangeJson = HttpUtil.get(api("exchanges/" + vhost + "/" + exchange));
        return JsonUtil.parseObject(exchangeJson, ExchangeBean.class);
    }

    private String api(String path) {
        return "http://localhost:" + new AmqpServiceConfiguration().getAmqpAdminPort() + "/api/" + path;
    }

}
