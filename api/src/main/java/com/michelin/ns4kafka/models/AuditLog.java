package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.michelin.ns4kafka.controllers.ApplyStatus;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class AuditLog {

    private String user;
    private boolean admin;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Date date;
    private String kind;
    private ObjectMeta metadata;
    private ApplyStatus operation;
    private Object before;
    private Object after;

}
