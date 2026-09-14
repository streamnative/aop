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
package io.streamnative.pulsar.handlers.amqp.admin.impl;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Proxy;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for AMQP admin resource helpers.
 */
public class BaseResourcesTest {

    @DataProvider(name = "matchingBrokerUrls")
    public Object[][] matchingBrokerUrls() {
        return new Object[][]{
                {"http://broker-1:8080", null, null, null,
                        "http://broker-1:8080", null, null, null},
                {null, "https://broker-1:8443", null, null,
                        null, "https://broker-1:8443", null, null},
                {null, null, "pulsar://broker-1:6650", null,
                        null, null, "pulsar://broker-1:6650", null},
                {null, null, null, "pulsar+ssl://broker-1:6651",
                        null, null, null, "pulsar+ssl://broker-1:6651"}
        };
    }

    @Test(dataProvider = "matchingBrokerUrls")
    public void testMatchesOwnerBrokerWithNonBlankUrl(
            String reportHttpUrl, String reportHttpUrlTls, String reportBrokerUrl, String reportBrokerUrlTls,
            String lookupHttpUrl, String lookupHttpUrlTls, String lookupBrokerUrl, String lookupBrokerUrlTls) {
        LoadManagerReport report = newReport(
                reportHttpUrl, reportHttpUrlTls, reportBrokerUrl, reportBrokerUrlTls);
        LookupData lookupData = newLookupData(
                lookupHttpUrl, lookupHttpUrlTls, lookupBrokerUrl, lookupBrokerUrlTls);

        assertTrue(BaseResources.matchesOwnerBroker(report, lookupData));
    }

    @DataProvider(name = "missingOrDifferentBrokerUrls")
    public Object[][] missingOrDifferentBrokerUrls() {
        return new Object[][]{
                {null, null},
                {"", ""},
                {" ", " "},
                {null, "http://broker-1:8080"},
                {"http://broker-1:8080", null},
                {"http://broker-1:8080", "http://broker-2:8080"}
        };
    }

    @Test(dataProvider = "missingOrDifferentBrokerUrls")
    public void testDoesNotMatchOwnerBrokerWithoutEqualNonBlankUrl(String reportUrl, String lookupUrl) {
        LoadManagerReport report = newReport(reportUrl, null, null, null);
        LookupData lookupData = newLookupData(lookupUrl, null, null, null);

        assertFalse(BaseResources.matchesOwnerBroker(report, lookupData));
    }

    @Test
    public void testMissingTlsUrlsDoNotOverrideDifferentPlaintextUrls() {
        LoadManagerReport report = newReport(
                "http://broker-1:8080", null, "pulsar://broker-1:6650", null);
        LookupData lookupData = newLookupData(
                "http://broker-2:8080", null, "pulsar://broker-2:6650", null);

        assertFalse(BaseResources.matchesOwnerBroker(report, lookupData));
    }

    private static LoadManagerReport newReport(
            String httpUrl, String httpUrlTls, String brokerUrl, String brokerUrlTls) {
        return (LoadManagerReport) Proxy.newProxyInstance(
                LoadManagerReport.class.getClassLoader(),
                new Class<?>[]{LoadManagerReport.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getWebServiceUrl" -> httpUrl;
                    case "getWebServiceUrlTls" -> httpUrlTls;
                    case "getPulsarServiceUrl" -> brokerUrl;
                    case "getPulsarServiceUrlTls" -> brokerUrlTls;
                    case "toString" -> "TestLoadManagerReport";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static LookupData newLookupData(
            String httpUrl, String httpUrlTls, String brokerUrl, String brokerUrlTls) {
        return new LookupData("broker-id", brokerUrl, brokerUrlTls, httpUrl, httpUrlTls);
    }
}
