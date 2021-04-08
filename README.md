ns4kafka
=======================

**ns4kafka** brings to Kafka a new deployment model following the best practices from Kubernetes :  

- **Namespace isolation.** You can manage your own Kafka resources (Topics, Connects, ACLs) within your namespace and you don't see Kafka resources managed by other namespaces
- **Desired state.** You define how the deployed resources should look like when deployed using **Resource descriptors** defined with Yaml files.
- **Apply / Idempotence.** You can execute the same deployment multiple times using the command line tool ``kafkactl apply`` and unmodified resources are simply ignored.
- **Server side validation.** Validation models allows Kafka OPS to define custom validators to enforce validation rules such as forcing Topic config ``min.insync.replica`` to a specific value or Connect config ``connect.class`` to a predifined list of allowed Connector classes.

On top of that, this models unlocks interesting features allowing you to have a more robust CI/CD process :  

- **Dry-run mode.** Executes the deployment without actually persisting or triggering resource creations or modifications.
- **Diff mode.** Displays the changes that would occur on resources compared to the current state.

ns4kafka is built on top of 2 components : an API and a CLI.

The API exposes all the required controllers to list, create and delete Kafka resources. It must be deployed and managed by Kafka administrators.  
The CLI is, much like kubectl, a wrapper on the API to let any user or CI/CD pipeline deploy Kafka resources using yaml descriptors. It is made available to any project who needs to manage Kafka resources.

## Quick start

````yaml
# descriptor.yml
---
apiVersion: v1
kind: Topic
metadata:
  name: project1.topic1
spec:
  replicationFactor: 3
  partitions: 3
  configs:
    min.insync.replicas: '2'
    cleanup.policy: delete
    retention.ms: '60000'
---
apiVersion: v1
kind: Connector
metadata:
  name: project1.connect1
spec:
  connector.class: org.apache.kafka.connect.file.FileStreamSinkConnector
  tasks.max: '1'
  topics: connect-test
  file: /tmp/project1.topic1.out
  consumer.override.sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="<user>" password="<passord>"

````
Simply run ``kafkactl apply --namespace project1 --file descriptor.yml`` and the Topic and Connect configuration will be deployed.

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

