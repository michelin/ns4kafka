package com.michelin.ns4kafka.cli.models;

import java.beans.JavaBean;

@JavaBean
public class Resource {

    private final String apiVersion = "v1";
    private String kind;
    private Object metadata;
    private Object spec;
	public String getKind() {
		return kind;
	}
	public Object getSpec() {
		return spec;
	}
	public void setSpec(Object spec) {
		this.spec = spec;
	}
	public Object getMetadata() {
		return metadata;
	}
	public void setMetadata(Object metadata) {
		this.metadata = metadata;
	}
	public void setKind(String kind) {
		this.kind = kind;
	}
}
