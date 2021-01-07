package com.michelin.ns4kafka.models;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public abstract class KafkaResource {
    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
}
