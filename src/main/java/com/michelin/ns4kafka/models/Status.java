package com.michelin.ns4kafka.models;

import static com.michelin.ns4kafka.utils.enums.Kind.STATUS;

import com.michelin.ns4kafka.utils.enums.Kind;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
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
    private String reason;
    private StatusDetails details;
    private int code;

    /**
     * Constructor.
     */
    @Builder
    public Status(StatusPhase status, String message, HttpStatus httpStatus, StatusDetails details) {
        super("v1", STATUS);
        this.status = status;
        this.message = message;
        this.reason = httpStatus.getReason();
        this.details = details;
        this.code = httpStatus.getCode();
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
}
