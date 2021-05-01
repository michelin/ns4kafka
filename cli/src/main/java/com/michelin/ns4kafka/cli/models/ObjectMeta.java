package com.michelin.ns4kafka.cli.models;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Introspected
@Getter
@Setter
@ToString
public class ObjectMeta {
	private String name;
	private String namespace;
	private String cluster;
	private Map<String,String> labels;
}
