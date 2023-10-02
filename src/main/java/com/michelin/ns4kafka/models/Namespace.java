package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Namespace.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class Namespace {
    private final String apiVersion = "v1";
    private final String kind = "Namespace";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private NamespaceSpec spec;

    /**
     * Namespace spec.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NamespaceSpec {
        @NotBlank
        private String kafkaUser;
        @Builder.Default
        private List<String> connectClusters = List.of();
        private TopicValidator topicValidator;
        private ConnectValidator connectValidator;
    }
}