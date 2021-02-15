package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Connector {
    private final String apiVersion = "v1";
    private final String kind = "Connector";
    @Valid
    @NotNull
    private ObjectMeta metadata;

    @NotNull
    private Map<String,String> spec;

    @Schema(accessMode = Schema.AccessMode.READ_ONLY)
    private ConnectStatus status;



    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Server-side",accessMode = Schema.AccessMode.READ_ONLY)
    public static class ConnectStatus {
        private ConnectPhase phase;
        private String message;
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

    }
    public enum ConnectPhase {
        Pending,
        Success,
        Failed
    }
}
