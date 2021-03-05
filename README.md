ns4kafka
=======================
Namespaces on top of Kafka Broker, Kafka Connect and Schema Registry

Table of Contents
=================
  * [Table of Contents](#table-of-contents)
  * [Key features](#key-features)
  * Install
  * Configure
    * Security
    * Storage
    * Clusters
  * API Specification

# Key features
- Desired state API
  - Mimics K8S principles
  - Easy to use
    - View : GET /api/namespaces/\<ns-name\>/topics/\<topic-name\>
    - List : GET /api/namespaces/\<ns-name\>/topics
    - Create : POST /api/namespaces/\<ns-name\>/topics
- Self-service
  - [Topics](#topic-creation-request)
  - Schemas, Connects, KafkaUsers
- Configuration validation that can differ for each namespace
  - Enforce any configuration for any resource
    - min.insync.replica = 2
    - partitions between 3 and 10
  - Multiple validators
- Isolation between Kafka Users within a cluster
- [Security model](#namespace-access-control-list)
- Disk Quota management
  - Enforce limits per namespace
  - Provide usage metrics
- [Cross Namespace ACLs](#grant-read-access-to-another-namespace)
- Multi cluster


Example namespace:  
````json
{
  "name": "namespace-project1",
  "cluster": "kafka-dev",
  "defaulKafkatUser": "u_project1",
  "topicValidator": {
    "validationConstraints": {
      "replication.factor": {
        "validation-type": "Range",
        "min": 1,
        "max": 1
      },
      "partitions": {
        "validation-type": "Range",
        "min": 3,
        "max": 10
      },
      "cleanup.policy": {
        "validation-type": "ValidList",
        "validStrings": [
          "delete",
          "compact"
        ]
      },
      "retention.ms": {
        "validation-type": "Range",
        "min": 1000,
        "max": 3600000
      }
    }
  }
}
````

### Namespace Access Control List
````json
{
  "apiVersion": "v1",
  "kind": "AccessControlEntry",
  "metadata": {
    "name": "namespace-project1-6b062011",
    "namespace": "namespace-project1",
    "cluster": "kafka-dev",
    "labels": {
      "grantor": "admin"
    }
  },
  "spec": {
    "resourceType": "TOPIC",
    "resource": "project1.",
    "resourcePatternType": "PREFIXED",
    "permission": "OWNER",
    "grantedTo": "namespace-project1"
  }
}
````

### Topic creation request
````
POST /api/namespaces/namespace-project1/topics HTTP/1.1
Host: localhost:8080
X-Gitlab-Token: xxxxxxxxxx
Content-Type: application/json
Content-Length: 250

{
  "metadata": {
    "name": "project1.topic1"
  },
  "spec": {
    "replicationFactor": 1,
    "partitions": 3,
    "configs": {
      "cleanup.policy": "delete",
      "min.insync.replicas": "1",
      "retention.ms": "10000"
    }
  }
}
````

### Grant Read access to another namespace
````
POST /api/namespaces/namespace-project1/acls HTTP/1.1
Host: localhost:8080
X-Gitlab-Token: xxxxxxxxxx
Content-Type: application/json
Content-Length: 195

{
  "spec": {
    "resourceType": "TOPIC",
    "resource": "project1.topic1",
    "resourcePatternType": "LITERAL",
    "permission": "READ",
    "grantedTo": "namespace-anotherproject"
  }
}
````

