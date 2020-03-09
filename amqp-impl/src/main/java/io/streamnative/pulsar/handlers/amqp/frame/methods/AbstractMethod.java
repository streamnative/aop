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
package io.streamnative.pulsar.handlers.amqp.frame.methods;

/**
 * Base class for AMQP methods.
 */
public abstract class AbstractMethod implements Method {

    /**
     * Returns the name of the methods class, e.g. connection.
     *
     * @return The name.
     */
    protected abstract String getMethodClassName();

    /**
     * Returns the name of the methods method, e.g. start-ok.
     *
     * @return The name.
     */
    protected abstract String getMethodMethodName();

    /**
     * Returns a unique description of this methods by joining class and method name with a dot
     * e.g. connection.start-ok.
     *
     * @return The unique name.
     */
    public final String getMethodClassAndMethodName() {
        return String.format("%s.%s", getMethodClassName(), getMethodMethodName()).intern();
    }

    @Override
    public String toString() {
        return String.format("%s.%s", getMethodClassName(), getMethodMethodName()).intern();
    }

}
