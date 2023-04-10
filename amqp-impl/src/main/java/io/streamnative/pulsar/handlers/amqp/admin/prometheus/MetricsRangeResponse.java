package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.SamplesBean;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class MetricsRangeResponse {

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
            private List<List<String>> values;

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

    public Map<String, List<SamplesBean>> getTopicValueMap() {
        return data.result.stream()
                .collect(Collectors.toMap(resultBean -> resultBean.getMetric().getTopic(),
                        v -> {
                            List<List<String>> values = v.getValues();
                            return values.stream()
                                    .map(strList -> {
                                        SamplesBean samplesBean = new SamplesBean();
                                        String time = strList.get(0) + "000";
                                        String count = strList.get(1);
                                        samplesBean.setTimestamp(Double.parseDouble(time));
                                        samplesBean.setSample(Double.parseDouble(count));
                                        return samplesBean;
                                    })
                                    .sorted(Comparator.comparing(SamplesBean::getTimestamp, Comparator.reverseOrder()))
                                    .collect(Collectors.toList());
                        }, (o, o2) -> {
                            if (o.size() >= o2.size()) {
                                return o;
                            }
                            return o2;
                        }));
    }

    public List<SamplesBean> getValueMap() {
        return getTopicValueMap().values()
                .stream()
                .flatMap(Collection::stream)
                .sorted(Comparator.comparing(SamplesBean::getTimestamp, Comparator.reverseOrder()))
                .collect(Collectors.toList());
    }
}
