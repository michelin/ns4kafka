package com.michelin.ns4kafka.model;

import static com.michelin.ns4kafka.util.enumation.Kind.DELETE_RECORDS_RESPONSE;

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
 * Delete records response.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class DeleteRecordsResponse extends MetadataResource {
    @Valid
    @NotNull
    private DeleteRecordsResponseSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public DeleteRecordsResponse(Metadata metadata, DeleteRecordsResponseSpec spec) {
        super("v1", DELETE_RECORDS_RESPONSE, metadata);
        this.spec = spec;
    }

    /**
     * Delete records response specification.
     */
    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DeleteRecordsResponseSpec {
        private String topic;
        private int partition;
        private Long offset;
    }
}
