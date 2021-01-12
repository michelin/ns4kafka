package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.TopicValidator;
import lombok.*;

import java.util.List;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Namespace {
    private String name;
    private String cluster;
    private List<ResourceSecurityPolicy> policies;
    private TopicValidator topicValidator;
    private int diskQuota;
    // TODO default Kafka User is given maximum grants and is synchronized
    private final String defaulKafkatUser="FDW_OLS_01";
    // TODO others users are namespace defined and must be "managed" (ACL must be defined and maintained)
    //private List<User> users; MVP 35

}
