package com.michelin.ns4kafka.models;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
public class Namespace {
    private String namespace;
    private String adminLdapGroup;
    private Map<String, String> quotas;
}
