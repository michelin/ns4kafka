package com.michelin.ns4kafka.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

@Getter
@Setter
@NoArgsConstructor
public class KafkaCluster {
    private String name;
    private String boostrapServers;
    private SecurityProtocol securityProtocol;
    private ScramMechanism scramMechanism;
    private String username;
    private String password;
}
