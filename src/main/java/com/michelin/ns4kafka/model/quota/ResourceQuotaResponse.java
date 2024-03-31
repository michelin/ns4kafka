package com.michelin.ns4kafka.model.quota;

import static com.michelin.ns4kafka.util.enumation.Kind.RESOURCE_QUOTA_RESPONSE;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Resource quota response.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ResourceQuotaResponse extends MetadataResource {
    @Valid
    @NotNull
    private ResourceQuotaResponseSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public ResourceQuotaResponse(Metadata metadata, ResourceQuotaResponseSpec spec) {
        super("v1", RESOURCE_QUOTA_RESPONSE, metadata);
        this.spec = spec;
    }

    /**
     * Resource quota response spec.
     */
    @Getter
    @Builder
    @ToString
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResourceQuotaResponseSpec {
        private String countTopic;
        private String countPartition;
        private String diskTopic;
        private String countConnector;
        private String consumerByteRate;
        private String producerByteRate;
    }
}
