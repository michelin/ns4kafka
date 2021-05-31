package com.michelin.ns4kafka.cli.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;
import java.util.Map;

@Introspected
@Getter
@Setter
@ToString
@Builder
public class ObjectMeta {
	private String name;
	private String namespace;
	private String cluster;
	private Map<String,String> labels;
	@JsonFormat(shape = JsonFormat.Shape.STRING)
	private Date creationTimestamp;
}
