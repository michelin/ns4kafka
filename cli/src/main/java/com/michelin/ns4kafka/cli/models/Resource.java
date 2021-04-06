package com.michelin.ns4kafka.cli.models;

public class Resource {

    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    private Object spec;
	public String getApiVersion() {
		return apiVersion;
	}
	public void setApiVersion(String apiVersion) {
		this.apiVersion = apiVersion;
	}
	public String getKind() {
		return kind;
	}
	public void setKind(String kind) {
		this.kind = kind;
	}
	public ObjectMeta getMetadata() {
		return metadata;
	}
	public void setMetadata(ObjectMeta metadata) {
		this.metadata = metadata;
	}
	public Object getSpec() {
		return spec;
	}
	public void setSpec(Object spec) {
		this.spec = spec;
	}

}
