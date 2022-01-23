package vn.cloud.springkafkamonitor.broker;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class BrokerServiceTests {

    @Autowired
    private BrokerService brokerService;

    @Test
    void describeCluster() {
        this.brokerService.describeCluster();
    }

    @Test
    void describeClusterConfig() {
        this.brokerService.describeBrokerConfig("1");
    }

    @Test
    void listTopics() {
        this.brokerService.listTopics();
    }

    @Test
    void describeAcls() {
        this.brokerService.describeAcls();
    }

    @Test
    void describeTopic() {
        this.brokerService.describeTopic("topic1");
    }

    @Test
    void describeTopicConfig() {
        this.brokerService.describeTopicConfig("topic1");
    }

    @Test
    void consumerGroups() {
        this.brokerService.consumerGroups();
    }

    @Test
    void consumerGroupDescription() {
        this.brokerService.consumerGroupDescription("productGroup");
    }

}