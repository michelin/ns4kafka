package com.michelin.ns4kafka.models.quota;

import static com.michelin.ns4kafka.utils.enums.Kind.RESOURCE_QUOTA;

import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Resource quota.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ResourceQuota extends MetadataResource {
    @NotNull
    private Map<String, String> spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public ResourceQuota(Metadata metadata, Map<String, String> spec) {
        super("v1", RESOURCE_QUOTA, metadata);
        this.spec = spec;
    }

    /**
     * Resource quota spec keys.
     */
    @Getter
    @AllArgsConstructor
    public enum ResourceQuotaSpecKey {
        COUNT_TOPICS("count/topics"),
        COUNT_PARTITIONS("count/partitions"),
        DISK_TOPICS("disk/topics"),
        COUNT_CONNECTORS("count/connectors"),
        USER_PRODUCER_BYTE_RATE("user/producer_byte_rate"),
        USER_CONSUMER_BYTE_RATE("user/consumer_byte_rate");

        private final String key;

        @Override
        public String toString() {
            return key;
        }
    }
}
