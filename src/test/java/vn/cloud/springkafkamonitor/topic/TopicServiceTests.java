package vn.cloud.springkafkamonitor.topic;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class TopicServiceTests {

    @Autowired
    private TopicService topicService;

    @Test
    void getLatestRecords() {
        this.topicService.getLatestRecords("topic1");
    }

    @Test
    void getNLatestRecords() {
        this.topicService.getNLatestRecords("topic1", 0, 7);
    }

    @Test
    void getTopicInfo() {
        this.topicService.getTopicInfo("topic1");
    }

    @Test
    void avc() {
        this.topicService.getNRecordsFromOffset("topic1", 0, 1, 7);
    }


}