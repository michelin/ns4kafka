# Table of Contents

  * [Access Control Entries](#access-control-entries)
  * [Topics](#topics)
  * [Connectors](#connectors)
  * [Consumer Group](#consumer-groups)
  * [Namespaces](#namespaces-admin-only)
  * [Role Bindings](#role-bindings-admin-only)

A list of resources exist in the ``example`` folder:
You can create them with the command: ``kafkactl apply -f path-to-descriptor.yml``

## Resources

### Access control entries
For an existing **namespace** the **access control entry** resource define a Kafka ACL on a specific resource  

List of resource types in the *resourceTypes* section:   
TOPIC, CONNECT, SCHEMA and GROUP

List of functions available in the *verbs* section:   
GET, POST, PUT and DELETE  

In the following examples :  
- the *nspce-acl1* **access control entry** gives the current namespace the *OWNER* permission for all topics starting with *nspce_*  
- the *nspce-acl2* **access control entry** gives the current namespace the *OWNER* permission for all connects starting with *nspce_connect*  
- the *nspce-acl3* **access control entry** gives the *other-nspce* namespace the *READ* permission for a topic named *nspce_first_topic*


````yaml
# accesControlEntries.yml
---
apiVersion: v1
kind: AccessControlEntry
metadata:
  name: nspce-acl0
  namespace: nspce-0
  cluster: cluster-0
spec:
  resourceType: TOPIC
  resource: nspce_
  resourcePatternType: PREFIXED
  permission: OWNER
  grantedTo: nspce-0
---
apiVersion: v1
kind: AccessControlEntry
metadata:
  name: nspce-acl2
  namespace: nspce-0
  cluster: cluster-0
spec:
  resourceType: CONNECT
  resource: nspce_connect
  resourcePatternType: PREFIXED
  permission: OWNER
  grantedTo: other-nspce
---
apiVersion: v1
kind: AccessControlEntry
metadata:
  name: nspce-acl3
  namespace: nspce-0
  cluster: cluster-0
spec:
  resourceType: TOPIC
  resource: nspce_first_topic
  resourcePatternType: LITERAL
  permission: READ
  grantedTo: other-nspce
````

#### Available functions
- ``apply`` to create the role binding
- ``get`` to list all role bindings or describe a specific role binding
- ``delete`` to delete a role binding

### Topics

For an existing **namespace** the **topic** resource define a Kafka topic

The spec of the topic must respect the **topicValidator** field from the Namespace

```yaml
# topic.yml
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

```

#### Available functions
- ``apply`` to create the topic
- ``get`` to list all role bindings or describe a specific topic
- ``delete`` to delete a topic
- ``delete-records`` to delete the record of a topic

### Connectors

For an existing **namespace** the **connector** resource define a Kafka connect

The spec of the connector must respect the **connectValidator** field from the Namespace

``` yaml
# connector.yml
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
```
#### Available functions
- ``apply`` to create the connector
- ``get`` to list all role bindings or describe a specific connector
- ``delete`` to delete a connector

### Consumer groups

Its possible to give to consumer groups Owner, Read and Write access to a Consumer Group thank to **Access Control Entry** by set up the **resourceType** GROUP

#### Available functions
- ``reset-offsets`` to change the offsets of the consumer group

### Namespaces (Admin only)
The **namespace** resource defines the name of namespace and the cluster to deploy it.
In the *spec* section, additional information must be given: 
- the Kafka user associated to the namespace
- a list of Kafka Connect clusters where connectors should be deployed
- a list of validators used when creating topics and connectors. This list can be customized according to the needs of the namespace

````yaml
# namespace.yml
---
  apiVersion: v1
  kind: Namespace
  metadata:
    name: nspce-0
    cluster: cluster-0
    labels:
      support-group: ldap-group-of-my-nspce
      contacts: user1.nspce@nspce.com,user2.nspce@nspce.com
  spec:
    kafkaUser: nspce-user
    connectClusters:
      - connect-cluster-1
      - connect-cluster-2
    topicValidator:
      validationConstraints:
        min.insync.replicas:
          validation-type: Range
          min: 2
          max: 2
        cleanup.policy:
          validation-type: ValidString
          validStrings:
          - delete
          - compact
        replication.factor:
          validation-type: Range
          min: 3
          max: 3
        partitions:
          validation-type: Range
          min: 3
          max: 6
        retention.ms:
          validation-type: Range
          min: 60000
          max: 604800000
    connectValidator:
      validationConstraints:
        connector.class:
          validation-type: ValidString
          validStrings:
          - io.confluent.connect.jdbc.JdbcSourceConnector
          - io.confluent.connect.jdbc.JdbcSinkConnector
          - com.github.jcustenborder.kafka.connect.spooldir.SpoolDirAvroSourceConnector
          - org.apache.kafka.connect.file.FileStreamSinkConnector
          - io.confluent.connect.http.HttpSinkConnector
        key.converter:
          validation-type: NonEmptyString
        value.converter:
          validation-type: NonEmptyString
      sourceValidationConstraints:
        producer.override.sasl.jaas.config:
          validation-type: NonEmptyString
      sinkValidationConstraints:
        consumer.override.sasl.jaas.config:
          validation-type: NonEmptyString
      classValidationConstraints:
        io.confluent.connect.jdbc.JdbcSinkConnector:
          db.timezone:
            validation-type: NonEmptyString
````

#### Available functions
- ``apply`` to create a namespace
- ``get`` to list all namespace or describe a specific namespace
- ``delete`` to delete a namespace

### Role bindings (Admin only)
For an existing **namespace** and a given Gitlab group, the **role binding** resource defines authorized functions for a list of resources.

List of resource types in the *resourceTypes* section:
topics, connects, acls

List of functions available in the *verbs* section:
GET, POST, PUT and DELETE

In the following example, all users belonging to the *Gitlab* group *access-ns4kfk/user-ns4kfk* will be authorized to do GET and POST on topics and connects resources:

````yaml
# roleBinding.yml
---
apiVersion: v1
kind: RoleBinding
metadata:
  name: nspce-rb1
  namespace: nspce-0
  cluster: cluster-0
spec:
  role:
    resourceTypes:
    - topics
    - connects
    verbs:
    - GET
    - POST
  subject:
    subjectType: GROUP
    subjectName: access-ns4kfk/user-ns4kfk
````

#### Available functions
- ``apply`` to create the role binding
- ``get`` to list all role bindings or describe a specific role binding
- ``delete`` to delete a role binding
