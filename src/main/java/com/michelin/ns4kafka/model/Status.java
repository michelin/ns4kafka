/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.model;

import static com.michelin.ns4kafka.util.enumation.Kind.STATUS;

import com.fasterxml.jackson.annotation.JsonValue;
import com.michelin.ns4kafka.util.enumation.Kind;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Status. */
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
     *
     * @param status The status
     * @param message The message
     * @param reason The reason
     * @param details The details
     * @param code The code
     */
    public Status(StatusPhase status, String message, String reason, StatusDetails details, int code) {
        super("v1", STATUS);
        this.status = status;
        this.message = message;
        this.reason = reason;
        this.details = details;
        this.code = code;
    }

    /**
     * Constructor.
     *
     * @param status The status
     * @param message The message
     * @param httpStatus The http status
     * @param details The details
     */
    @Builder
    public Status(StatusPhase status, String message, HttpStatus httpStatus, StatusDetails details) {
        this(status, message, httpStatus.getReason(), details, httpStatus.getCode());
    }

    /** Status details. */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StatusDetails {
        private String name;
        private Kind kind;
        private List<String> causes;
    }

    /** Status phase. */
    @Getter
    @AllArgsConstructor
    public enum StatusPhase {
        SUCCESS("Success"),
        FAILED("Failed");

        private final String name;

        @JsonValue
        @Override
        public String toString() {
            return name;
        }
    }
}
