package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
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
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

        public static TopicStatus ofSuccess(String message){
            return TopicStatus.builder()
                    .phase(TopicPhase.Success)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }
        public static TopicStatus ofFailed(String message){
            return TopicStatus.builder()
                    .phase(TopicPhase.Failed)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }
        public static TopicStatus ofPending(){
            return Topic.TopicStatus.builder()
                    .phase(Topic.TopicPhase.Pending)
                    .message("Awaiting processing by executor")
                    .build();
        }
    }
    public enum TopicPhase {
        Pending,
        Success,
        Failed
    }

}
