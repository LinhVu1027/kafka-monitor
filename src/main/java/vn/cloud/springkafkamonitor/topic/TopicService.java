package vn.cloud.springkafkamonitor.topic;

import java.util.List;
import java.util.Map;

public interface TopicService {
    Map<String, List<ConsumerRecordDto<Object, Object>>> getLatestRecords(String topic);
    List<ConsumerRecordDto<Object, Object>> getNLatestRecords(String topic, int partition, int n);
    List<ConsumerRecordDto<Object, Object>> getNRecordsFromOffset(String topic, int partition, long recordOffset, int n);
    TopicDto getTopicInfo(String topic);

}
