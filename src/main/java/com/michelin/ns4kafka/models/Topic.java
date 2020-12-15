package com.michelin.ns4kafka.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@NoArgsConstructor
@Getter
@Setter
public class Topic extends KafkaResource {
    private String cluster;
    private String name;
    private int replicationFactor;
    private int partitions;
    private Map<String,String> config;

}
