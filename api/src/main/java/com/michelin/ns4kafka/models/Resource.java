package com.michelin.ns4kafka.models;

import lombok.*;

@AllArgsConstructor
@Data
public abstract class Resource {
    private final String version;
    private final String kind;
    private ObjectMeta metadata;
}
