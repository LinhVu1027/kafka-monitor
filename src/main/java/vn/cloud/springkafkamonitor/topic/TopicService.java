package vn.cloud.springkafkamonitor.topic;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface TopicService {
    void getLatestRecords(String topic);
    ConsumerRecords<Object, Object> getNLatestRecords(String topic, int partition, int n);
    ConsumerRecords<Object, Object> getNLatestRecordsFromOffset(String topic, int partition, long recordOffset, int n);
    TopicDto getTopicInfo(String topic);

}
