package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.ConsumerGroupState;

@Getter
@NoArgsConstructor
public class ConsumerGroupListingDto {
    private String groupId;
    private boolean isSimpleConsumerGroup;
    private ConsumerGroupState state;

    public static ConsumerGroupListingDto from(ConsumerGroupListing consumerGroup) {
        ConsumerGroupListingDto dto = new ConsumerGroupListingDto();
        dto.groupId = consumerGroup.groupId();
        dto.isSimpleConsumerGroup = consumerGroup.isSimpleConsumerGroup();
        dto.state = consumerGroup.state().orElse(ConsumerGroupState.UNKNOWN);
        return dto;
    }
}
