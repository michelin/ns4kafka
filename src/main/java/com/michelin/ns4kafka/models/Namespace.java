package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.models.security.ResourceSecurityPolicy;
import lombok.*;

import java.util.List;
import java.util.Map;

@Data
public class Namespace {
    private String name;
    private String adminLdapGroup;
    private List<ResourceSecurityPolicy> policies;
    private Map<String, String> quotas;
}
