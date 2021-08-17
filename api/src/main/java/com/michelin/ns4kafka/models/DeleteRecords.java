package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;
import org.apache.kafka.common.TopicPartition;

import javax.validation.constraints.NotNull;
import java.util.Map;

@Introspected
@Getter
@Setter
@EqualsAndHashCode(callSuper=false)
public class DeleteRecords extends Resource{

    @Builder
    public DeleteRecords(ObjectMeta metadata, DeleteRecordsStatus status) {
        super("v1","DeleteRecords", metadata);
        this.status = status;
    }
    private DeleteRecordsStatus status;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class DeleteRecordsStatus {
        private boolean success;
        private String errorMessage;
        private Map<TopicPartition, Long> lowWaterMarks;

    }
}
