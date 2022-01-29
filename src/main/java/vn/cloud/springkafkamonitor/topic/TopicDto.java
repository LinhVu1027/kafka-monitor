package vn.cloud.springkafkamonitor.topic;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.TreeMap;

@Getter
@NoArgsConstructor
public class TopicDto {
    private String name;
    private Map<Integer, PartitionDto> partitions = new TreeMap<>();

    public TopicDto(String name) {
        this.name = name;
    }

    public void setPartitions(Map<Integer, PartitionDto> partitions) {
        this.partitions = partitions;
    }
}
