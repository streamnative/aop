package io.streamnative.pulsar.handlers.amqp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

/**
 * HttpUtil.
 */
public class HttpUtil {

    private final static OkHttpClient client = new OkHttpClient();

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public static CompletableFuture<Void> putAsync(String url, Map<String, Object> params) {
        RequestBody requestBody;
        try {
            requestBody = RequestBody.create(JsonUtil.toString(params), JSON);
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
        Request request = new Request.Builder()
                .url(url)
                .put(requestBody)
                .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                if (!response.isSuccessful()) {
                    future.completeExceptionally(new IOException("Unexpected code " + response));
                    return;
                }
                future.complete(null);
            }
        });
        return future;
    }

    public static CompletableFuture<Void> postAsync(String url, Map<String, Object> params) {
        RequestBody requestBody;
        try {
            requestBody = RequestBody.create(JsonUtil.toString(params), JSON);
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                if (!response.isSuccessful()) {
                    future.completeExceptionally(new IOException("Unexpected code " + response));
                    return;
                }
                future.complete(null);
            }
        });
        return future;
    }

}
