package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
public class TopicListingDto {
    private final List<String> internals = new ArrayList<>();
    private final List<String> externals = new ArrayList<>();

    public static TopicListingDto from(Map<String, TopicListing> topicListings) {
        TopicListingDto dto = new TopicListingDto();
        for (TopicListing value : topicListings.values()) {
            if (value.isInternal()) {
                dto.internals.add(value.name());
            } else {
                dto.externals.add(value.name());
            }
        }
        return dto;
    }
}
