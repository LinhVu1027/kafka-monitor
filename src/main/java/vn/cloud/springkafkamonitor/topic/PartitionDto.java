package vn.cloud.springkafkamonitor.topic;

import java.util.ArrayList;
import java.util.List;

public class PartitionDto {
    private int id;
    private Integer replicaLeaderId;
    private List<ReplicaDto> replicas = new ArrayList<>();

    public PartitionDto(int id, Integer replicaLeaderId) {
        this.id = id;
        this.replicaLeaderId = replicaLeaderId;
    }

    public void addReplica(ReplicaDto replica) {
        this.replicas.add(replica);
    }

    public int getId() {
        return id;
    }

    public static class ReplicaDto {
        private int id;
        private boolean leader;
        private boolean inSync;
        private boolean offline;

        public ReplicaDto(int id, boolean leader, boolean inSync, boolean offline) {
            this.id = id;
            this.leader = leader;
            this.inSync = inSync;
            this.offline = offline;
        }
    }
}
