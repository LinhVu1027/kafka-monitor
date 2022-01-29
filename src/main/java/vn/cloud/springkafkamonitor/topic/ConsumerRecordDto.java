package vn.cloud.springkafkamonitor.topic;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor
public class ConsumerRecordDto<K, V> {
    private long offset;
    private long timestamp;
    private TimestampType timestampType;
    private List<HeaderDto> headers;
    private K key;
    private V value;
    private Optional<Integer> leaderEpoch;

    public static <K, V> ConsumerRecordDto<K, V> from(ConsumerRecord<K, V> record) {
        ConsumerRecordDto<K, V> dto = new ConsumerRecordDto<>();
        dto.offset = record.offset();
        dto.timestamp = record.timestamp();
        dto.timestampType = record.timestampType();
        dto.headers = Arrays.stream(record.headers().toArray()).map(HeaderDto::from).collect(Collectors.toList());
        dto.key = record.key();
        dto.value = record.value();
        dto.leaderEpoch = record.leaderEpoch();
        return dto;
    }

    @Getter
    @NoArgsConstructor
    private static class HeaderDto {
        private String key;
        private String value;

        private static HeaderDto from(Header header) {
            HeaderDto dto = new HeaderDto();
            dto.key = header.key();
            dto.value = new String(header.value(), StandardCharsets.UTF_8);
            return dto;
        }
    }
}
