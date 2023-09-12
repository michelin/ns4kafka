package com.michelin.ns4kafka.models.schema;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.List;

@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class Schema {
    private final String apiVersion = "v1";
    private final String kind = "Schema";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private SchemaSpec spec;

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaSpec {
        private Integer id;
        private Integer version;
        private String schema;

        @Builder.Default
        private SchemaType schemaType = SchemaType.AVRO;

        @Builder.Default
        private Compatibility compatibility = Compatibility.GLOBAL;

        private List<Reference> references;

        @Getter
        @Setter
        @Builder
        @Serdeable
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Reference {
            private String name;
            private String subject;
            private Integer version;
        }
    }

    public enum Compatibility {
        GLOBAL,
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE,
        NONE
    }

    public enum SchemaType {
        AVRO,
        JSON,
        PROTOBUF
    }
}
