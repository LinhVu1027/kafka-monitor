package vn.cloud.springkafkamonitor.broker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor
public class ClusterDto {
    private String clusterId;
    private List<NodeDto> nodes;
    private NodeDto controller;

    public static ClusterDto from(String clusterId, Collection<Node> nodes, Node controller) {
        ClusterDto dto = new ClusterDto();
        dto.clusterId = clusterId;
        dto.nodes = nodes.stream().map(NodeDto::from).collect(Collectors.toList());
        dto.controller = NodeDto.from(controller);
        return dto;
    }
}
