package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import java.util.HashMap;
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
        Map<String, List<Double>> resultMap = data.result.stream()
                .collect(Collectors.toMap(resultBean -> resultBean.getMetric().getTopic(),
                        DataBean.ResultBean::getValue, (o, o2) -> {
                            if (o.size() >= o2.size()) {
                                return o;
                            }
                            return o2;
                        }));
        Map<String, Double> valToMap = new HashMap<>(resultMap.size());
        resultMap.forEach((k, v) -> {
            if (v.isEmpty()) {
                valToMap.put(k, 0.0);
                return;
            }
            valToMap.put(k, v.get(v.size() - 1));
        });
        return valToMap;
    }
}
