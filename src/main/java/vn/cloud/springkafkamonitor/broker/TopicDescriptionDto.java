package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor
public class TopicDescriptionDto {
    private String name;
    private boolean internal;
    private String topicId;
    private final List<TopicPartitionInfoDto> partitions = new ArrayList<>();

    public static TopicDescriptionDto from(TopicDescription topicDescription) {
        TopicDescriptionDto dto = new TopicDescriptionDto();
        dto.name = topicDescription.name();
        dto.internal = topicDescription.isInternal();
        dto.topicId = topicDescription.topicId().toString();
        dto.partitions.addAll(topicDescription.partitions().stream().map(TopicPartitionInfoDto::from).collect(Collectors.toList()));
        return dto;
    }
}
