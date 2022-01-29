package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Set;

@Getter
@NoArgsConstructor
public class ConsumerGroupDto {
    private String groupId;
    private Set<ConsumerMemberDto> members;
    private String coordinator;

    public ConsumerGroupDto(String groupId, Set<ConsumerMemberDto> members, String coordinator) {
        this.groupId = groupId;
        this.members = members;
        this.coordinator = coordinator;
    }

    @Getter
    @NoArgsConstructor
    public static class ConsumerMemberDto {
        private String memberId;
        private String clientId;
        private Set<TopicPartitionOffsetDto> topicPartitions;

        public ConsumerMemberDto(String memberId, String clientId, Set<TopicPartitionOffsetDto> topicPartitions) {
            this.memberId = memberId;
            this.clientId = clientId;
            this.topicPartitions = topicPartitions;
        }
    }

    @Getter
    @NoArgsConstructor
    public static class TopicPartitionOffsetDto {
        private String topic;
        private int partition;
        private long earliestOffset;
        private long committedOffset;
        private long latestOffset;

        public TopicPartitionOffsetDto(String topic, int partition, long earliestOffset, long latestOffset, long committedOffset) {
            this.topic = topic;
            this.partition = partition;
            this.earliestOffset = earliestOffset;
            this.latestOffset = latestOffset;
            this.committedOffset = committedOffset;
        }

        public long getLag() {
            return this.latestOffset - this.committedOffset;
        }
    }
}
