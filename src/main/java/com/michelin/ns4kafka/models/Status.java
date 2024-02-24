package com.michelin.ns4kafka.models;

import static com.michelin.ns4kafka.utils.enums.Kind.STATUS;

import com.michelin.ns4kafka.utils.enums.Kind;
import io.micronaut.core.annotation.Introspected;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Status.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class Status extends Resource {
    private StatusPhase status;
    private String message;
    private StatusReason reason;
    private StatusDetails details;
    private int code;

    /**
     * Constructor.
     */
    @Builder
    public Status(StatusPhase status, String message, StatusReason reason, StatusDetails details, int code) {
        super("v1", STATUS);
        this.status = status;
        this.message = message;
        this.reason = reason;
        this.details = details;
        this.code = code;
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
        private Kind kind;
        private List<String> causes;
    }

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
}
