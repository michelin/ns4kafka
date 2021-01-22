# kafka-namespace-service
Namespaces on top of Kafka Broker, Kafka Connect and Schema Registry

## Key features
- Desired state API
  - Mimics K8S principles
  - Easy to use
    - View : GET /api/namespaces/\<ns-name\>/topics/\<topic-name\>
    - List : GET /api/namespaces/\<ns-name\>/topics/
    - Create : POST /api/namespaces/\<ns-name\>/topics/\<topic-name\>
- Self-service
  - Topics, Schemas, Connects, KafkaUsers
- Configuration validation that can differ for each namespace
  - Enforce any configuration for any resource
    - min.insync.replica = 2
    - partitions between 3 and 10
  - Multiple validators
- Isolation between Kafka Users within a cluster
- Security model
- Disk Quota management
  - Enforce limits per namespace
  - Provide usage metrics
- Cross Namespace ACLs
- Multi cluster


Example namespace:  
````
{
  "name": "namespace-project1",
  "cluster": "kafka-dev",
  // Kafka User to "link" namespace ACLs (Topic and CGoup)
  "defaulKafkatUser": "u_project1",
  "policies": [
    {
      "resourceType": "TOPIC",
      "resource": "project1.",
      "resourcePatternType": "PREFIXED",
      "securityPolicy": "OWNER"
    },
    {
      "resourceType": "CONNECT",
      "resource": "project1-", // Allow creation of Kafka Connects starting with "project1-"
      "resourcePatternType": "PREFIXED",
      "securityPolicy": "OWNER"
    },
    {
      "resourceType": "GROUP",
      "resource": "project1-",
      "resourcePatternType": "PREFIXED",
      "securityPolicy": "READ"
    },
    {
      "resourceType": "GROUP",
      "resource": "connect-project1-", // Consumer group required for Kafka Connect
      "resourcePatternType": "PREFIXED",
      "securityPolicy": "READ"
    }
  ],
  // Topic Validation constraints to check for this namespace
  "topicValidator": {
    "validationConstraints": {
      "min.insync.replicas": {
        "validation-type": "Range",
        "min": 1,
        "max": 1
      },
      "cleanup.policy": {
        "validation-type": "ValidList",
        "validStrings": [
          "delete",
          "compact"
        ]
      },
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
      "retention.ms": {
        "validation-type": "Range",
        "min": 1000,
        "max": 1000000
      }
    }
  },
  "diskQuota": 5, // Quota available for this namesapce (5Gb)
}
````

Topic creation request :  

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


