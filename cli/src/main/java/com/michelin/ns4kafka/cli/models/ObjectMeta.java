package com.michelin.ns4kafka.cli.models;

import java.util.Map;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;

@Introspected
@Getter
@Setter
public class ObjectMeta {
	private String name;
	private String namespace;
	private String cluster;
	private Map<String,String> labels;
}
