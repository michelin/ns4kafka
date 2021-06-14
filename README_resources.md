ns4kafka
=======================


# CLI specification

The list of function can be accessed with ``kafkactl`` without argument.

Here is a list of the most useful:
- ``apply`` to create a resource
- ``get`` to know the configuration of a deployed resource
- ``api-resources`` to know the supported resource by the api

## Functions

### Apply

### Get

### Delete

### Diff

### Reset offsets

### Delete records

### Api resources

# Examples

A list of resources exist in the ``example`` folder:
You can create them with the command: ``kafkactl apply -f path-to-descriptor.yml``

## Resources

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
- ``delete`` to delete a namespace

### Role binding (Admin only)
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

### Access control entries
For an existing **namespace** the **access control entry** resource define a Kafka ACL on a specific resource  

List of resource types in the *resourceTypes* section:   
topics, connects, acls  

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

### Connectors

### Consumer groups



````yaml
# namespace.yml
---
  apiVersion: v1
  kind: Namespace
  metadata:
    name: nspce
    cluster: ckfkgbl0
    labels:
      support-group: ldap-group-of-my-nspce
      contacts: user1.nspce@nspce.com,user2.nspce@nspce.com
  spec:
    kafkaUser: nspce-user
    connectClusters:
      - nspce-cluster-1
      - nspce-cluster-2
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