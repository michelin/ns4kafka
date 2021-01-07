package com.michelin.ns4kafka.models;

import lombok.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
@Getter
@Setter
public class Topic extends KafkaResource {

    private TopicSpec spec;
    private TopicStatus status;

    @Builder
    public Topic(TopicSpec spec, TopicStatus status, String apiVersion, String kind, ObjectMeta metadata){
        super(apiVersion,kind,metadata);
        this.spec=spec;
        this.status=status;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class TopicSpec {
        private int replicationFactor;
        private int partitions;
        private Map<String,String> configs;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class TopicStatus {
        private TopicPhase phase;
        private String message;
    }
    public enum TopicPhase {
        Pending,
        Success,
        Failed
    }

}
