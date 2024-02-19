package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Status.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class Status {
    private static final String apiVersion = "v1";
    public static final String kind = "Status";
    private StatusPhase status;
    private String message;
    private StatusReason reason;
    private StatusDetails details;
    private int code;

    /**
     * Status phase.
     */
    public enum StatusPhase {
        Success,
        Failed
    }

    /**
     * Status reason.
     */
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

    /**
     * Status details.
     */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StatusDetails {
        private String name;
        private String kind;
        private List<String> causes;
    }
}
