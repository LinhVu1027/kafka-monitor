package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor
public class TopicPartitionInfoDto {
    private int partition;
    private int leader;
    private final List<Integer> replicas = new ArrayList<>();
    private final List<Integer> isr = new ArrayList<>();

    public static TopicPartitionInfoDto from(TopicPartitionInfo partitionInfo) {
        TopicPartitionInfoDto dto = new TopicPartitionInfoDto();
        dto.partition = partitionInfo.partition();
        dto.leader = partitionInfo.leader().id();
        dto.replicas.addAll(partitionInfo.replicas().stream().map(Node::id).collect(Collectors.toList()));
        dto.isr.addAll(partitionInfo.isr().stream().map(Node::id).collect(Collectors.toList()));
        return dto;
    }
}
