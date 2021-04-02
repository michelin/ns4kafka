package com.michelin.ns4kafka.cli.models;

import java.util.Map;

public class Resource {

    private final String apiVersion = "v1";
    private String kind;
    private ObjectMeta metadata;
    private Object spec;
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

	public class ObjectMeta {
		private String name;
		private String namespace;
		private String cluster;
		private Map<String,String> labels;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getNamespace() {
			return namespace;
		}
		public void setNamespace(String namespace) {
			this.namespace = namespace;
		}
		public String getCluster() {
			return cluster;
		}
		public void setCluster(String cluster) {
			this.cluster = cluster;
		}
		public Map<String, String> getLabels() {
			return labels;
		}
		public void setLabels(Map<String, String> labels) {
			this.labels = labels;
		}
	}
}
