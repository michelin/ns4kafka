package com.michelin.ns4kafka.models.quota;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Data
@Builder
@Introspected
@AllArgsConstructor
@NoArgsConstructor
public class ResourceQuota {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Kind of resource
     */
    private final String kind = "ResourceQuota";

    /**
     * Resource quota metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * Resource quota specification
     */
    @Valid
    @NotNull
    private Map<String, String> spec;

    @Getter
    @Introspected
    @AllArgsConstructor
    public enum ResourceQuotaSpecKey {
        COUNT_TOPICS("count/topics"),
        COUNT_PARTITIONS("count/partitions"),
        DISK_TOPICS("disk/topics"),
        COUNT_CONNECTORS("count/connectors");

        private final String key;

        @Override
        public String toString() {
            return key;
        }
    }
}
