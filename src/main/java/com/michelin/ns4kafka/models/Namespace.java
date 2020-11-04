package com.michelin.ns4kafka.models;

import lombok.*;

import java.util.Map;

@Data
public class Namespace {
    private String name;
    private String adminLdapGroup;
    private Map<String, String> quotas;
}
