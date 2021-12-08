package com.michelin.ns4kafka.cli.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Status {
    private String apiVersion = "v1";
    private String kind = "Status";

    private StatusPhase status;

    private String message;
    private String reason;

    private StatusDetails details;

    private int code;

    @Builder
    @NoArgsConstructor
    @Data
    public static class StatusDetails {
        private String name;
        private String kind;
        private List<String> causes;
        @JsonCreator
        public StatusDetails(@JsonProperty("name") String name, @JsonProperty("kind") String kind, @JsonProperty("causes") List<String> causes)
        {
            this.name = name;
            this.kind = kind;
            this.causes = causes;
        }
    }

    public enum StatusPhase {
        Success,
        Failed
    }
}
