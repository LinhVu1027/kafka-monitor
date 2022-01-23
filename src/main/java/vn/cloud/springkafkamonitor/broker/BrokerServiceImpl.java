package vn.cloud.springkafkamonitor.broker;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
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
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class BrokerServiceImpl implements BrokerService {
    private Map<String, Object> configs;

    public BrokerServiceImpl(KafkaProperties kafkaProperties) {
        this.configs = kafkaProperties.buildAdminProperties();
    }

    private AdminClient createAdmin() {
        Map<String, Object> configs2 = new HashMap<>(this.configs);
        return AdminClient.create(configs2);
    }

    public void describeCluster() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            DescribeClusterResult clusterMetadata = kafkaAdmin.describeCluster();
            String clusterId = clusterMetadata.clusterId().get();
            Collection<Node> nodes = clusterMetadata.nodes().get();
            Node controller = clusterMetadata.controller().get();

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void describeBrokerConfig(String brokerId) {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
            DescribeConfigsResult configsResult = kafkaAdmin.describeConfigs(Collections.singletonList(configResource));
            System.out.println(Thread.currentThread().getName());
            configsResult.all().whenComplete((result, error) -> {
                System.out.println(Thread.currentThread().getName());
                System.out.println("hehe");
            });

            ConfigResource configResource1 = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, brokerId);
            DescribeConfigsResult configsResult1 = kafkaAdmin.describeConfigs(Collections.singletonList(configResource1));
            Map<ConfigResource, Config> configResourceConfigMap1 = configsResult1.all().get();

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void describeAcls() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            DescribeAclsResult describeAclsResult = kafkaAdmin.describeAcls(AclBindingFilter.ANY);
            Collection<AclBinding> aclBindings = describeAclsResult.values().get();

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void listTopics() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ListTopicsResult topicsMetadata = kafkaAdmin.listTopics();
            Map<String, TopicListing> stringTopicListingMap = topicsMetadata.namesToListings().get();

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void describeTopic(String topic) {
        try (AdminClient kafkaAdmin = createAdmin()) {
            DescribeTopicsResult topicsMetadata = kafkaAdmin.describeTopics(Collections.singletonList(topic));
            TopicDescription topicDescription = topicsMetadata.values().get(topic).get();

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void describeTopicConfig(String topic) {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            DescribeConfigsResult configsResult = kafkaAdmin.describeConfigs(Collections.singletonList(configResource));
            Map<ConfigResource, Config> configResourceConfigMap = configsResult.all().get();

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void consumerGroups() {
        try (AdminClient kafkaAdmin = createAdmin()) {
            ListConsumerGroupsResult consumerGroups = kafkaAdmin.listConsumerGroups();
            Collection<ConsumerGroupListing> valid = consumerGroups.valid().get();
            Collection<Throwable> errors = consumerGroups.errors().get();
            Collection<ConsumerGroupListing> all = consumerGroups.all().get();

            System.out.println("haha");
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void consumerGroupDescription(String groupId) {
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
            ConsumerGroupDto result = new ConsumerGroupDto(consumerGroupDescription.groupId(), members, consumerGroupDescription.coordinator().toString());

            System.out.println("haha");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
