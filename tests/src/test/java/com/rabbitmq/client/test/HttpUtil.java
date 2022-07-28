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
package com.rabbitmq.client.test;

import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * HttpUtils.
 */
@Slf4j
public class HttpUtil {

    private static OkHttpClient client = new OkHttpClient();

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public static String get(String url) throws IOException {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .get();
        Request request = builder.build();
        try (Response response = client.newCall(request).execute()) {
            String responseStr = response.body().string();
            if (log.isDebugEnabled()) {
                log.debug("get request ur: {} response: {}", url, responseStr);
            }
            return responseStr;
        }
    }

    public static String put(String url, Map<String, Object> params) throws IOException {
        RequestBody requestBody = RequestBody.create(JsonUtil.toString(params), JSON);

        Request.Builder builder = new Request.Builder()
                .url(url)
                .put(requestBody);

        Request request = builder.build();
        try (Response response = client.newCall(request).execute()) {
            String responseStr = response.body().string();
            if (log.isDebugEnabled()) {
                log.debug("put request ur: {} response: {}", url, responseStr);
            }
            return responseStr;
        }
    }

    public static String post(String url, Map<String, Object> params) throws IOException {
        RequestBody requestBody = RequestBody.create(JsonUtil.toString(params), JSON);

        Request.Builder builder = new Request.Builder()
                .url(url)
                .post(requestBody);

        Request request = builder.build();
        try (Response response = client.newCall(request).execute()) {
            String responseStr = response.body().string();
            if (log.isDebugEnabled()) {
                log.debug("post request ur: {} response: {}", url, responseStr);
            }
            return responseStr;
        }
    }

    public static String delete(String url) throws IOException {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .delete();

        Request request = builder.build();
        try (Response response = client.newCall(request).execute()) {
            String responseStr = response.body().string();
            if (log.isDebugEnabled()) {
                log.debug("delete request ur: {} response: {}", url, responseStr);
            }
            return responseStr;
        }
    }

}
