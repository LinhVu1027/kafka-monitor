package vn.cloud.springkafkamonitor.broker;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class BrokerServiceImpl implements BrokerService {
    private final Map<String, Object> configs;

    public BrokerServiceImpl(KafkaProperties kafkaProperties) {
        this.configs = kafkaProperties.buildAdminProperties();
    }

    private AdminClient createAdmin() {
        Map<String, Object> configs2 = new HashMap<>(this.configs);
        return AdminClient.create(configs2);
    }

    public ClusterDto describeCluster() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            DescribeClusterResult clusterMetadata = kafkaAdmin.describeCluster();
            String clusterId = clusterMetadata.clusterId().get();
            Collection<Node> nodes = clusterMetadata.nodes().get();
            Node controller = clusterMetadata.controller().get();
            return ClusterDto.from(clusterId, nodes, controller);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> describeBrokerConfig(String brokerId) {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
            DescribeConfigsResult configsResult = kafkaAdmin.describeConfigs(Collections.singletonList(configResource));
            Config config = configsResult.values().get(configResource).get();

            Map<String, String> brokerConfig = new TreeMap<>();
            for (ConfigEntry entry : config.entries()) {
                brokerConfig.put(entry.name(), entry.value());
            }
            return brokerConfig;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void describeAcls() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            DescribeAclsResult describeAclsResult = kafkaAdmin.describeAcls(AclBindingFilter.ANY);
            Collection<AclBinding> aclBindings = describeAclsResult.values().get();
            System.out.println("haha");
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public TopicListingDto listTopics() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ListTopicsResult topicsMetadata = kafkaAdmin.listTopics();
            Map<String, TopicListing> topicListings = topicsMetadata.namesToListings().get();
            return TopicListingDto.from(topicListings);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public TopicDescriptionDto describeTopic(String topic) {
        try (AdminClient kafkaAdmin = createAdmin()) {
            DescribeTopicsResult topicsMetadata = kafkaAdmin.describeTopics(Collections.singletonList(topic));
            TopicDescription topicDescription = topicsMetadata.values().get(topic).get();
            return TopicDescriptionDto.from(topicDescription);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> describeTopicConfig(String topic) {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            DescribeConfigsResult configsResult = kafkaAdmin.describeConfigs(Collections.singletonList(configResource));
            Config config = configsResult.values().get(configResource).get();

            Map<String, String> topicConfig = new TreeMap<>();
            for (ConfigEntry entry : config.entries()) {
                topicConfig.put(entry.name(), entry.value());
            }
            return topicConfig;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<ConsumerGroupListingDto> consumerGroups() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ListConsumerGroupsResult consumerGroups = kafkaAdmin.listConsumerGroups();
            Collection<ConsumerGroupListing> valid = consumerGroups.valid().get();
            Collection<Throwable> errors = consumerGroups.errors().get();
            Collection<ConsumerGroupListing> all = consumerGroups.all().get();

            return all.stream().map(ConsumerGroupListingDto::from).collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public ConsumerGroupDto consumerGroupDescription(String groupId) {
        try (AdminClient adminClient = createAdmin()) {
            // ConsumerGroup's member
            ConsumerGroupDescription consumerGroupDescription = adminClient.describeConsumerGroups(Collections.singletonList(groupId))
                    .describedGroups().get(groupId).get();

            // ConsumerGroup's partition and the committed offset in each partition
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            // Additional information: latest offset of each partition
            Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
            for (TopicPartition tp : offsets.keySet()) {
                requestLatestOffsets.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(requestLatestOffsets).all().get();

            // Additional information: earliest offset of each partition
            Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();
            for (TopicPartition tp : offsets.keySet()) {
                requestEarliestOffsets.put(tp, OffsetSpec.earliest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets = adminClient.listOffsets(requestEarliestOffsets).all().get();

            // Build Dto
            Set<ConsumerGroupDto.ConsumerMemberDto> members = consumerGroupDescription.members().stream().map(member -> {
                Set<ConsumerGroupDto.TopicPartitionOffsetDto> partitions = member.assignment().topicPartitions().stream().map(tp -> {
                    long committedOffset = offsets.get(tp).offset();
                    long latestOffset = latestOffsets.get(tp).offset();
                    long earliestOffset = earliestOffsets.get(tp).offset();
                    return new ConsumerGroupDto.TopicPartitionOffsetDto(tp.topic(), tp.partition(), earliestOffset, latestOffset, committedOffset);
                }).collect(Collectors.toSet());
                return new ConsumerGroupDto.ConsumerMemberDto(member.consumerId(), member.clientId(), partitions);
            }).collect(Collectors.toSet());
            return new ConsumerGroupDto(consumerGroupDescription.groupId(), members, consumerGroupDescription.coordinator().toString());

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
