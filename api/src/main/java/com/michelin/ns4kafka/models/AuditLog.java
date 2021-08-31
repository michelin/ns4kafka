package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.controllers.ApplyStatus;
import io.micronaut.security.utils.SecurityService;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AuditLog {

    private SecurityService user;
    private String date;
    private String kind;
    private ObjectMeta metadata;
    private ApplyStatus operation;
    private Object before;
    private Object after;

}
