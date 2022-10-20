package com.michelin.ns4kafka.models.quota;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Getter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class ResourceQuotaResponse {
    private final String apiVersion = "v1";
    private final String kind = "ResourceQuotaResponse";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private ResourceQuotaResponseSpec spec;

    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ResourceQuotaResponseSpec {
        private String countTopic;
        private String countPartition;
        private String diskTopic;
        private String countConnector;
    }
}
