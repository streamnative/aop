package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.SamplesBean;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.Nullable;

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
        String instance = getInstance();
        return data.result.stream()
                .filter(resultBean -> {
                    if (instance == null) {
                        return true;
                    }
                    return resultBean.getMetric().getInstance().equals(instance);
                })
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
                        }));
    }

    private String getInstance() {
        String instance = null;
        if (data.result.size() > 1) {
            TreeMap<Integer, String> treeMap = data.result.stream().collect(
                    Collectors.toMap(resultBean -> resultBean.getValues().size(),
                            r -> r.getMetric().getInstance(),
                            (v1, v2) -> v1, TreeMap::new));
            instance = treeMap.lastEntry().getValue();
        }
        return instance;
    }

    public List<SamplesBean> getValueMap() {
        String instance = getInstance();
        return data.result.stream()
                .filter(resultBean -> {
                    if (instance == null) {
                        return true;
                    }
                    return resultBean.getMetric().getInstance().equals(instance);
                })
                .map(resultBean -> {
                    List<List<String>> values = resultBean.getValues();
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

                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
