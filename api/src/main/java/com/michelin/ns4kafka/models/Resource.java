package com.michelin.ns4kafka.models;

import lombok.*;

@AllArgsConstructor
@Getter
@Setter
@ToString
@RequiredArgsConstructor
public abstract class Resource {
    private final String version;
    private final String kind;
    private ObjectMeta metadata;
}
