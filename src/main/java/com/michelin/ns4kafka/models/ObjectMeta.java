package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;

import java.util.Date;
import java.util.Map;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class ObjectMeta {
    private String name;
    private String namespace;
    private String cluster;
    private Map<String,String> labels;
    private int generation;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Date creationTimestamp;
}
