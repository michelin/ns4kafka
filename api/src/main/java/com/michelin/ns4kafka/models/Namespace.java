package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

@Introspected
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Namespace {

    private final String apiVersion = "v1";
    private final String kind = "Namespace";
    @Valid
    @NotNull
    private ObjectMeta metadata;

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
