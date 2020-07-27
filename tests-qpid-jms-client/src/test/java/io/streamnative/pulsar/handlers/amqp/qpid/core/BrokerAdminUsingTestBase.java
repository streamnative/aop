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
package io.streamnative.pulsar.handlers.amqp.qpid.core;

import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * BrokerAdminUsingTestBase.
 */
@RunWith(QpidTestRunner.class)
public abstract class BrokerAdminUsingTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAdminUsingTestBase.class);
    @Rule
    public final TestName _testName = new TestName();
    @Rule
    public final Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

    private BrokerAdmin _brokerAdmin;

    public void init(final BrokerAdmin brokerAdmin)
    {
        _brokerAdmin = brokerAdmin;
    }

    public BrokerAdmin getBrokerAdmin()
    {
        return _brokerAdmin;
    }

    protected String getTestName()
    {
        return _testName.getMethodName();
    }
}
