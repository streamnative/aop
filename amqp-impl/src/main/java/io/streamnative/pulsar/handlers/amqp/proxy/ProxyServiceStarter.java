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
package io.streamnative.pulsar.handlers.amqp.proxy;

/**
 * The proxy service starter.
 */
public class ProxyServiceStarter {

    public ProxyServiceStarter(String[] args) throws Exception {
        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        ProxyService proxyService = new ProxyService(proxyConfig, null);
        proxyService.start();

        WebServer webServer = new WebServer(proxyConfig, null);
        webServer.start();
    }

    public static void main(String[] args) throws Exception {
        new ProxyServiceStarter(args);
    }

}
