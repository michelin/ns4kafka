package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

@Introspected
@Data
@EqualsAndHashCode(callSuper = true)
public class Namespace extends Resource{

    @Builder
    public Namespace(@NotNull ObjectMeta metadata, NamespaceSpec spec) {
        super("v1","Namespace", metadata);
        this.spec = spec;
    }

    @Valid
    @NotNull
    private NamespaceSpec spec;

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class NamespaceSpec {
        @NotBlank
        private String kafkaUser;
        private List<String> connectClusters = List.of();
        private TopicValidator topicValidator;
        private ConnectValidator connectValidator;
        //private ResourceQuota quota;
    }


}
