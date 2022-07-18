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
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Resource kind
     */
    private final String kind = "ResourceQuotaResponse";

    /**
     * Resource quota metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * Resource specifications
     */
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
        /**
         * The count quota for topics
         */
        private String countTopic;

        /**
         * The count quota for partitions
         */
        private String countPartition;

        /**
         * The count quota for connectors
         */
        private String countConnector;

    }
}
