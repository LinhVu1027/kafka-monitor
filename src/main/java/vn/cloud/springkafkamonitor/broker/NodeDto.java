package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.Node;

@Getter
@NoArgsConstructor
public class NodeDto {
    private int id;
    private String idString;
    private String host;
    private int port;
    private String rack;

    public static NodeDto from(Node node) {
        NodeDto dto = new NodeDto();
        dto.id = node.id();
        dto.idString = node.idString();
        dto.host = node.host();
        dto.port = node.port();
        dto.rack = node.rack();
        return dto;
    }
}
