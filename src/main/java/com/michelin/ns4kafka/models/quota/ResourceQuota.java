package com.michelin.ns4kafka.models.quota;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Resource quota.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class ResourceQuota {
    private final String apiVersion = "v1";
    private final String kind = "ResourceQuota";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @NotNull
    private Map<String, String> spec;

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
