package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class Status {
    private final String apiVersion = "v1";
    private final String kind = "Status";
    private StatusPhase status;
    private String message;
    private StatusReason reason;
    private StatusDetails details;
    private int code;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StatusDetails {
        private String name;
        private String kind;
        private List<String> causes;
    }

    public enum StatusPhase {
        Success,
        Failed
    }

    public enum StatusReason {
        BadRequest,
        Unauthorized,
        Forbidden,
        NotFound,
        AlreadyExists,
        Conflict,
        Invalid,
        Timeout,
        ServerTimeout,
        MethodNotAllowed,
        InternalError
    }
}
