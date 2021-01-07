package com.michelin.ns4kafka.models;

import lombok.*;

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
}
