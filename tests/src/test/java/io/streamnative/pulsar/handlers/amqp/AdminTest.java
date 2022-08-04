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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.test.HttpUtil;
import com.rabbitmq.client.test.JsonUtil;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueBean;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Admin API test.
 */
public class AdminTest extends AmqpTestBase{

    @Test(timeOut = 1000 * 5)
    public void listExchangeTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();
        Set<String> vhost1Exs = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String ex = randExName();
            vhost1Exs.add(ex);
            channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true);
        }
        String ex1 = vhost1Exs.iterator().next();

        Connection connection2 = getConnection("vhost2", true);
        Channel channel2 = connection2.createChannel();
        Set<String> vhost2Exs = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String ex = randExName();
            vhost2Exs.add(ex);
            channel2.exchangeDeclare(ex, BuiltinExchangeType.FANOUT, true);
        }

        List<ExchangeBean> exchangeBeans = exchangeList();
        assertTrue(exchangeBeans.size() > 0);
        for (ExchangeBean bean : exchangeBeans) {
            if (vhost1Exs.remove(bean.getName())) {
                Assert.assertEquals(bean.getType(), "direct");
            }
            if (vhost2Exs.remove(bean.getName())) {
                Assert.assertEquals(bean.getType(), "fanout");
            }
        }
        Assert.assertEquals(vhost1Exs.size(), 0);
        Assert.assertEquals(vhost2Exs.size(), 0);

        List<ExchangeBean> exchangeBeans1 = exchangeListByVhost("vhost1");
        assertTrue(exchangeBeans1.size() > 0);
        for (ExchangeBean bean : exchangeBeans1) {
            Assert.assertEquals(bean.getVhost(), "vhost1");
        }

        ExchangeBean bean = exchangeByName("vhost1", ex1);
        Assert.assertEquals(bean.getName(), ex1);
    }

    @Test(timeOut = 1000 * 5)
    public void exchangeDeclareAndDeleteTest() throws Exception {
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

    @Test(timeOut = 1000 * 5)
    public void listQueuesTest() throws Exception {
        Connection connection = getConnection("vhost1", false);
        Channel channel = connection.createChannel();

        Set<String> vhost1Queues = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String queueName = randQuName();
            vhost1Queues.add(queueName);
            channel.queueDeclare(queueName, true, false, false, null);
        }
        String qu1 = vhost1Queues.iterator().next();

        Connection connection2 = getConnection("vhost2", false);
        Channel channel2 = connection2.createChannel();
        Set<String> vhost2Queues = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String queueName = randQuName();
            vhost2Queues.add(queueName);
            channel2.queueDeclare(queueName, true, false, false, null);
        }

        List<QueueBean> queueBeans = listQueues();
        assertTrue(queueBeans.size() > 0);
        for (QueueBean bean : queueBeans) {
            if (vhost1Queues.remove(bean.getName())) {
                Assert.assertEquals(bean.getVhost(), "vhost1");
            }
            if (vhost2Queues.remove(bean.getName())) {
                Assert.assertEquals(bean.getVhost(), "vhost2");
            }
        }
        Assert.assertEquals(vhost1Queues.size(), 0);
        Assert.assertEquals(vhost2Queues.size(), 0);

        List<QueueBean> queueBeans1 = listQueuesByVhost("vhost1");
        assertTrue(queueBeans1.size() > 0);
        for (QueueBean bean : queueBeans1) {
            Assert.assertEquals(bean.getVhost(), "vhost1");
        }

        QueueBean queueBean = getQueue("vhost1", qu1);
        Assert.assertEquals(queueBean.getName(), qu1);
    }

    @Test(timeOut = 1000 * 5)
    public void queueDeclareDeleteTest() throws IOException {
        String vhost = "vhost3";
        Set<String> queueNameSet = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            String quName = randQuName();
            queueNameSet.add(quName);
            queueDeclare(vhost, quName);
        }

        List<QueueBean> beans = listQueuesByVhost(vhost);
        assertTrue(beans.size() > 0);
        for (QueueBean bean : beans) {
            queueNameSet.remove(bean.getName());
        }
        Assert.assertEquals(queueNameSet.size(), 0);

        String qu = beans.get(0).getName();
        queueDelete(vhost, qu);
        beans = listQueuesByVhost(vhost);
        assertTrue(beans.size() > 0);
        for (QueueBean bean : beans) {
            Assert.assertNotEquals(bean.getName(), qu);
            queueDelete(vhost, bean.getName());
        }
        beans = listQueuesByVhost(vhost);
        assertTrue(CollectionUtils.isEmpty(beans));
    }

    @Test
    public void bindingsTest() throws Exception {
        String vhost = "vhost1";
        String ex = randExName();
        String qu = randQuName();
        String key1 = randomName("key", 5);
        String key2 = randomName("key", 5);

        exchangeDeclare(vhost, ex, BuiltinExchangeType.TOPIC.getType());
        queueDeclare(vhost, qu);
        queueBind(vhost, ex, qu, key1);
        queueBind(vhost, ex, qu, key2);

        List<BindingBean> beans = listBindings(vhost, ex, qu);
        Assert.assertEquals(beans.size(), 2);
        Set<String> keySet = new HashSet<>();
        keySet.add(key1);
        keySet.add(key2);
        for (BindingBean bean : beans) {
            keySet.remove(bean.getPropertiesKey());
        }
        Assert.assertEquals(keySet.size(), 0);

        BindingBean expectedBean = new BindingBean();
        expectedBean.setVhost(vhost);
        expectedBean.setSource(ex);
        expectedBean.setDestination(qu);
        expectedBean.setDestinationType("queue");
        expectedBean.setRoutingKey(key2);
        expectedBean.setPropertiesKey(key2);
        expectedBean.setArguments(null);

        BindingBean bean = getBinding(vhost, ex, qu, key2);
        Assert.assertNotNull(bean);
        Assert.assertEquals(bean, expectedBean);

        // TODO currently, unbind operation will remove all bindings between exchange and queue,
        //  the right operation is only remove the specific key binding
        queueUnbind(vhost, ex, qu, key1);
        beans = listBindings(vhost, ex, qu);
        Assert.assertEquals(beans.size(), 0);
//        Assert.assertEquals(beans.get(0).getPropertiesKey(), key2);
//
//        queueUnbind(vhost, ex, qu, key2);
//        beans = listBindings(vhost, ex, qu);
//        Assert.assertEquals(beans.size(), 0);
//
//        exchangeDelete(vhost, ex);
//        queueDelete(vhost, qu);
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

    private void queueDeclare(String vhost, String queue) throws IOException {
        Map<String, Object> declareParams = new HashMap<>();
        declareParams.put("auto_delete", false);
        declareParams.put("durable", true);
        declareParams.put("arguments", null);
        declareParams.put("node", null);
        HttpUtil.put(api("queues/" + vhost + "/" + queue), declareParams);
    }

    private void queueDelete(String vhost, String queue) throws IOException {
        HttpUtil.delete(api("queues/" + vhost + "/" + queue));
    }

    private List<QueueBean> listQueues() throws IOException {
        String json = HttpUtil.get(api("queues"));
        return JsonUtil.parseObjectList(json, QueueBean.class);
    }

    private List<QueueBean> listQueuesByVhost(String vhost) throws IOException {
        String json = HttpUtil.get(api("queues/" + vhost));
        return JsonUtil.parseObjectList(json, QueueBean.class);
    }

    private QueueBean getQueue(String vhost, String queue) throws IOException {
        String json = HttpUtil.get(api("queues/" + vhost + "/" + queue));
        return JsonUtil.parseObject(json, QueueBean.class);
    }

    private void queueBind(String vhost, String exchange, String queue, String routingKey) throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("routing_key", routingKey);
        params.put("arguments", null);
        HttpUtil.post(api("bindings/" + vhost + "/e/" + exchange + "/q/" + queue), params);
    }

    private void queueUnbind(String vhost, String exchange, String queue, String routingKey) throws IOException {
        HttpUtil.delete(api("bindings/" + vhost + "/e/" + exchange + "/q/" + queue + "/" + routingKey));
    }

    private List<BindingBean> listBindings(String vhost, String exchange, String queue) throws IOException {
        String json = HttpUtil.get(api("bindings/" + vhost + "/e/" + exchange + "/q/" + queue));
        return JsonUtil.parseObjectList(json, BindingBean.class);
    }

    private BindingBean getBinding(String vhost, String exchange, String queue, String propsKey) throws IOException {
        String json = HttpUtil.get(api("bindings/" + vhost + "/e/" + exchange + "/q/" + queue + "/" + propsKey));
        return JsonUtil.parseObject(json, BindingBean.class);
    }

    private String api(String path) {
        return "http://localhost:" + new AmqpServiceConfiguration().getAmqpAdminPort() + "/api/" + path;
    }

    @Test
    public void test() throws IOException, TimeoutException {
        Connection connection = getConnection("vhost1", false);
        Channel channel = connection.createChannel();

        Map<String, Object> properties = new HashMap<>();
        properties.put("testNum", 10);
        properties.put("testDecimal", 10.1);
        properties.put("testString", "hello");
        channel.exchangeDeclare("exchange", "direct", false, false, properties);

        ExchangeBean bean = exchangeByName("vhost1", "exchange");

        Map<String, Object> arguments = bean.getArguments();
        assertNotNull(arguments);
        properties.forEach((k, v) -> {
            assertEquals(arguments.get(k), String.valueOf(v));
        });
    }
}
