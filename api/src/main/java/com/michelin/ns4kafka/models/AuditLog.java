package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.controllers.ApplyStatus;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;

@Data
@AllArgsConstructor
public class AuditLog {

    private Optional<String> user;
    private boolean admin;
    private String date;
    private String kind;
    private ObjectMeta metadata;
    private ApplyStatus operation;
    private Object before;
    private Object after;

}
