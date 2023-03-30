package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class MetricsResponse {

    private String status;
    private DataBean data;

    @NoArgsConstructor
    @Data
    public static class DataBean {
        private String resultType;
        private List<ResultBean> result;

        @NoArgsConstructor
        @Data
        public static class ResultBean {
            private MetricBean metric;
            private List<Double> value;

            @NoArgsConstructor
            @Data
            public static class MetricBean {
                private String __name__;
                private String cluster;
                private String instance;
                private String job;
                private String namespace;
                private String subscription;
                private String topic;
            }
        }
    }

    @Data
    public static class Info {
        private String topic;
        private double value;
    }

    public Map<String, Double> getTopicValueMap() {
        String instance = getInstance();
        return data.result.stream()
                .filter(resultBean -> {
                    if (instance == null) {
                        return true;
                    }
                    return resultBean.getMetric().getInstance().equals(instance);
                })
                .collect(Collectors.toMap(resultBean -> resultBean.getMetric().getTopic(), v -> {
                    List<Double> value = v.getValue();
                    if (value.isEmpty()) {
                        return 0.0;
                    }
                    return value.get(value.size() - 1);
                }));
    }

    private String getInstance() {
        String instance = null;
        if (data.result.size() > 1) {
            TreeMap<Integer, String> treeMap = data.result.stream()
                    .collect(
                            Collectors.toMap(resultBean -> resultBean.getValue().size(),
                                    r -> r.getMetric().getInstance(),
                                    (v1, v2) -> v1, TreeMap::new));
            instance = treeMap.lastEntry().getValue();
        }
        return instance;
    }
}
