package vn.cloud.springkafkamonitor.broker;

public interface BrokerService {
    void describeCluster();
    void describeBrokerConfig(String brokerId);
    void listTopics();
    void describeTopic(String topic);
    void describeTopicConfig(String topic);
    void consumerGroups();
    void consumerGroupDescription(String groupId);
    void describeAcls();
}
