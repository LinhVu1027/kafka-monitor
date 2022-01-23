package vn.cloud.springkafkamonitor.topic;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

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
        ConsumerRecords<Object, Object> nLatestRecords = this.topicService.getNLatestRecords("topic1", 0, 7);
        System.out.println("haha");
    }

    @Test
    void getTopicInfo() {
        this.topicService.getTopicInfo("topic1");
    }

    @Test
    void avc() {
        ConsumerRecords<Object, Object> nLatestRecordsFromOffset = this.topicService.getNLatestRecordsFromOffset("topic1", 0, 1, 7);
        System.out.println("haha");
    }


}