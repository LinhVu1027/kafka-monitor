package vn.cloud.springkafkamonitor.broker;

import java.util.List;
import java.util.Map;

public interface BrokerService {
    ClusterDto describeCluster();
    Map<String, String> describeBrokerConfig(String brokerId);
    TopicListingDto listTopics();
    TopicDescriptionDto describeTopic(String topic);
    Map<String, String> describeTopicConfig(String topic);
    List<ConsumerGroupListingDto> consumerGroups();
    ConsumerGroupDto consumerGroupDescription(String groupId);
    void describeAcls();
}
