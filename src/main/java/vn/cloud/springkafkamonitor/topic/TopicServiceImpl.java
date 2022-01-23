package vn.cloud.springkafkamonitor.topic;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class TopicServiceImpl implements TopicService {
    private final ConsumerFactory<Object, Object> consumerFactory;

    @Override
    public void getLatestRecords(String topic) {
        try (Consumer<Object, Object> kafkaConsumer = this.consumerFactory.createConsumer()) {
            // Get the topic's partitions
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);

            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());

            // Get the latest offset of each partition
            Map<TopicPartition, Long> latestOffsets = kafkaConsumer.endOffsets(partitions);

            // Seek from the latest record offset of each partition
            kafkaConsumer.assign(partitions); // If not, IllegalStateException: No current assignment for partitionX will be thrown when `seek`
            for (TopicPartition partition : partitions) {
                long latestRecordOffset = latestOffsets.get(partition) - 1;
                kafkaConsumer.seek(partition, latestRecordOffset);
            }

            ConsumerRecords<Object, Object> polled = kafkaConsumer.poll(Duration.of(5L, ChronoUnit.SECONDS));
            System.out.println("haha");
        }
    }

    @Override
    public ConsumerRecords<Object, Object> getNLatestRecordsFromOffset(String topic, int partition, long recordOffset, int n) {
        Properties props = this.maxRecordsPerPoll(n);
        try (Consumer<Object, Object> kafkaConsumer = this.consumerFactory.createConsumer(null, null, null, props)) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            // Get this partition's earliest offset
            Long earliestOffset = kafkaConsumer.beginningOffsets(Collections.singletonList(topicPartition)).get(topicPartition);

            recordOffset = Math.max(recordOffset, earliestOffset);

            kafkaConsumer.assign(Collections.singletonList(topicPartition));
            kafkaConsumer.seek(topicPartition, recordOffset);

            return kafkaConsumer.poll(Duration.of(5L, ChronoUnit.SECONDS));
        }
    }

    @Override
    public ConsumerRecords<Object, Object> getNLatestRecords(String topic, int partition, int n) {
        Properties props = this.maxRecordsPerPoll(n);
        try (Consumer<Object, Object> kafkaConsumer = this.consumerFactory.createConsumer(null, null, null, props)) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            // Get this partition's latest offset and earliest offset
            Long latestOffset = kafkaConsumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
            Long earliestOffset = kafkaConsumer.beginningOffsets(Collections.singletonList(topicPartition)).get(topicPartition);

            // Get the record latest offset
            long recordLatestOffset = latestOffset - 1;

            // Seek n records from `recordLatestOffset - n + 1` to `recordLatestOffset`
            kafkaConsumer.assign(Collections.singletonList(topicPartition));
            kafkaConsumer.seek(topicPartition, Math.max(earliestOffset, recordLatestOffset - n + 1));

            return kafkaConsumer.poll(Duration.of(5L, ChronoUnit.SECONDS));
        }
    }

    @Override
    public TopicDto getTopicInfo(String topic) {
        try (Consumer<Object, Object> kafkaConsumer = this.consumerFactory.createConsumer()) {
            TopicDto topicDto = new TopicDto(topic);
            Map<Integer, PartitionDto> partitionDtos = new TreeMap<>();

            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                int partitionId = partitionInfo.partition();
                Integer replicaLeaderId = Optional.ofNullable(partitionInfo.leader()).map(Node::id).orElse(null);
                PartitionDto partitionDto = new PartitionDto(partitionId, replicaLeaderId);

                Set<Integer> inSyncReplicaIds = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toSet());
                Set<Integer> offlineReplicaIds = Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());
                for (Node replica : partitionInfo.replicas()) {
                    boolean leader = replicaLeaderId != null && partitionInfo.leader().id() == replica.id();
                    boolean inSync = inSyncReplicaIds.contains(replica.id());
                    boolean offline = offlineReplicaIds.contains((replica.id()));
                    partitionDto.addReplica(new PartitionDto.ReplicaDto(replica.id(), leader, inSync, offline));
                }

                partitionDtos.put(partitionDto.getId(), partitionDto);
            }
            topicDto.setPartitions(partitionDtos);
            return topicDto;
        }

    }

    private Properties maxRecordsPerPoll(int n) {
        Assert.notNull(this.consumerFactory, "A consumerFactory is required");
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(n));
        return props;
    }

}
