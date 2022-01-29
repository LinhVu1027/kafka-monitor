package vn.cloud.springkafkamonitor.broker;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/brokers")
@RequiredArgsConstructor
public class BrokerController {

    private final BrokerService brokerService;

    @GetMapping("/cluster")
    public ClusterDto describeCluster() {
        return this.brokerService.describeCluster();
    }

    @GetMapping("/acls")
    public void describeAcls() {
        this.brokerService.describeAcls();
    }

    @GetMapping("/{id}")
    public Map<String, String> describeBrokerConfig(@PathVariable String id) {
        return this.brokerService.describeBrokerConfig(id);
    }

    @GetMapping("/topics")
    public TopicListingDto listTopics() {
        return this.brokerService.listTopics();
    }

    @GetMapping("/topics/{topic}")
    public TopicDescriptionDto describeTopic(@PathVariable String topic) {
        return this.brokerService.describeTopic(topic);
    }

    @GetMapping("/topics/{topic}/config")
    public Map<String, String> describeTopicConfig(@PathVariable String topic) {
        return this.brokerService.describeTopicConfig(topic);
    }

    @GetMapping("/consumerGroups")
    public List<ConsumerGroupListingDto> consumerGroups() {
        return this.brokerService.consumerGroups();
    }

    @GetMapping("/consumerGroups/{id}")
    public ConsumerGroupDto consumerGroupDescription(@PathVariable String id) {
        return this.brokerService.consumerGroupDescription(id);
    }

}
