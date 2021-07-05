ns4kafka
=======================
[![GitHub release](https://img.shields.io/github/v/release/michelin/ns4kafka)](https://github.com/michelin/ns4kafka/releases)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/michelin/ns4kafka/Snapshot)](https://github.com/michelin/ns4kafka/actions/workflows/on_push_master.yml/)
[![GitHub issues](https://img.shields.io/github/issues/michelin/ns4kafka)](https://github.com/michelin/ns4kafka/issues)
[![SonarCloud Coverage](https://sonarcloud.io/api/project_badges/measure?project=michelin_ns4kafka&metric=coverage)](https://sonarcloud.io/component_measures/metric/coverage/list?id=michelin_ns4kafka)
[![SonarCloud Bugs](https://sonarcloud.io/api/project_badges/measure?project=michelin_ns4kafka&metric=bugs)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=michelin_ns4kafka)
[![Docker Pulls](https://img.shields.io/docker/pulls/twobeeb/ns4kafka?label=ns4kafka%20pulls&logo=Docker)](https://hub.docker.com/r/twobeeb/ns4kafka/tags)
[![Docker Pulls](https://img.shields.io/docker/pulls/twobeeb/kafkactl?label=kafkactl%20pulls&logo=Docker)](https://hub.docker.com/r/twobeeb/kafkactl/tags)

# Table of Contents
* [About the project](#about-the-project)
* [Key features](#key-features)
* [Quick Start](#quick-start-cli)
* [Install kafkactl CLI](#install-kafkactl-cli)


# About the Project
**ns4kafka** brings to Apache Kafka a new deployment model for your different Kafka resources following the best practices from Kubernetes :

- **Namespace isolation.** You can manage your own Kafka resources within your namespace, and you don't see Kafka resources managed by other namespaces.
  Isolation is provided by granting ownership on names and prefixes to Namespaces
- **Desired state.** You define how the deployed resources should look like and ns4kafka will align the Kafka cluster with your desired state.
- **Server side validation.** Customizable validation rules defined by Kafka OPS to enforce values on Topic configs (``min.insync.replica``, ``replication.factor``, ...) or Connect configs (``connect.class``, ``consumer.override.jaas``, ...).
- **Robust CLI for all your CI/CD needs.** The `kafkactl` command line tool lets you control your resources within your namespace.
  You can deploy resources, list or delete them, reset consumer groups and so on.
- **An evolving list of Resources.** As Kafka project teams, you can now become fully autonomous managing Kafka ``Topics``, ``Connectors``, ``Schemas``, ``AccessControlEntries`` and ``ConsumerGroups``. Kafka Admin are treated equaly only with different resources to manage : `Namespaces`, `RoleBindings`, `ResourceQuotas`, `ResourceValidators`,  `AccessControlEntries`, ...

ns4kafka is built on top of 2 components : an **API** and a **CLI**.

- The **ns4kafka** API exposes all the required controllers to list, create and delete Kafka resources. It must be deployed and managed by Kafka administrators.
- The **kafkactl** CLI is, much like K8S's kubectl, a wrapper on the API to let any user or CI/CD pipeline deploy Kafka resources using yaml descriptors. It is made available to any project who needs to manage Kafka resources.


# Key Features (WIP)
- [x] Namespace Isolation
  - [x] One or more Resource prefixes per Namespace
  - [x] Single Kafka Principal per Namespace
  - [ ] Multiple Kafka Principals
- [x] Self-service Kafka resources management
  - [x] Topics
  - [x] Connectors
  - [x] ACLs (applied to the Kafka Principal)
  - [x] Cross Namespace Grants
    - [x] Topic (Namespace1 grants Read and/or Write to Namespace2)
  - [ ] Schemas
  - [ ] Kafka Principals (passwords, byte-rates)
- [x] Validation rules
  - [x] Topics
  - [x] Connectors
  - [ ] Schemas
- [x] Multi cluster
- [x] Security
  - [ ] Authentication / Authorization
    - [x] Gitlab Access Token
    - [ ] LDAP
    - [ ] OIDC
  - [x] Role Bindings (Fine-Grained ACLs : User / Resource / Operation)
- [ ] Resource Quotas
  - [ ] Disk usage
  - [ ] Partition count
  - [ ] Topic count
  - [ ] Connectors count
- [ ] Detailed audit log

# Quick start CLI

*The following examples demonstrates ns4kafka for a namespace which is owner of <b>test.\*</b> resources.*

### Create a Topic
```yaml
# topic.yml
---
apiVersion: v1
kind: Topic
metadata:
  name: test.topic1
spec:
  replicationFactor: 3
  partitions: 3
  configs:
    min.insync.replicas: '2'
    cleanup.policy: delete
    retention.ms: '60000'
```
````console
user@local:/home/user$ kafkactl apply -f topic.yml
Success Topic/test.topic1 (created)
# deploy twice
user@local:/home/user$ kafkactl apply -f topic.yml
Success Topic/test.topic1 (unchanged)
````
### Update a Topic
```yaml
# topic.yml
---
apiVersion: v1
kind: Topic
metadata:
  name: test.topic1
spec:
  replicationFactor: 3
  partitions: 3
  configs:
    min.insync.replicas: '2'
    cleanup.policy: delete
    retention.ms: '86400000' # Retention increased from 60s to 1d
```
````console
# diff mode is great to verify impacts beforehand
user@local:/home/user$ kafkactl diff -f topic.yml
---Topic/test.topic1-LIVE
+++Topic/test.topic1-MERGED
  configs:
    min.insync.replicas: '2'
    cleanup.policy: delete
-   retention.ms: '60000'
+   retention.ms: '86400000'

user@local:/home/user$ kafkactl apply -f topic.yml
Success Topic/test.topic1 (changed)
````

### Create an invalid Topic
#### Invalid Config
````yaml
# topic.yml
...
configs:
  min.insync.replicas: 'MinInWhat?'
...
````
````console
user@local:/home/user$ kafkactl apply -f topic.yml
Failed Topic/test.topic1 [Invalid value for 'retention.ms' : Value must be a Number]
# You should always dry-run first.
user@local:/home/user$ kafkactl apply -f topic.yml --dry-run
Failed Topic/test.topic1 [Invalid value for 'retention.ms' : Value must be a Number]
````
#### Invalid Ownership
````yaml
# topic.yml
...
metadata:
  name: production.topic1 # Recall we are owner of test.*
...
````
````console
user@local:/home/user$ kafkactl apply -f topic.yml
Failed Topic/production.topic1 [Invalid value for 'name' : Namespace not OWNER of this topic]
````
### Deploy a Connector
```yaml
# connector.yml
---
apiVersion: v1
kind: Connector
metadata:
  name: test.connect1
spec:
  connectCluster: local # This reference would be provided by your Kafka admin
  config:
    connector.class: org.apache.kafka.connect.file.FileStreamSinkConnector
    tasks.max: '1'
    topics: test-topic1
    file: /tmp/test-topic1.out
    # Unrelated: You should probably have this if running connect workers in multi-tenant environment
    consumer.override.sasl.jaas.config: o.a.k.s.s.ScramLoginModule required username="<user>" password="<password>";
```
````console
user@local:/home/user$ kafkactl apply -f connector.yml
Success Connector/test.connect1 (created)
````
### Forbidden Connector class
Connect Validation rules defined by your Kafka Admin for your Namespace
```yaml
# connector.yml
...
  config:
    connector.class: io.confluent.connect.hdfs.HdfsSinkConnector
...
```
````console
user@local:/home/user$ kafkactl apply -f connector.yml
Failed Connector/test.connect1 [Invalid value for 'connector.class' : String must be one of: 
org.apache.kafka.connect.file.FileStreamSinkConnector,
io.confluent.connect.jdbc.JdbcSinkConnector]
````
### Other useful commands
````console
# List all resources
user@local:/home/user$ kafkactl get all
Topics
  NAME              AGE
  test.topic1       10 minutes
Connectors
  NAME              AGE
  test.connect1     moments ago

# Describe a single resource
user@local:/home/user$ kafkactl get topic test.topic1 -oyaml
---
apiVersion: v1
kind: Topic
metadata:
  name: test.topic1
spec:
  replicationFactor: 3
  ...

# Delete a resource
user@local:/home/user$ kafkactl delete topic test.topic1
Success Topic/test.topic1 (deleted)

user@local:/home/user$ kafkactl delete connector test.connect1
Success Connector/test.connect1 (deleted)

# Deploy an entire folder
user@local:/home/user$ kafkactl apply -f /home/user/ # Applies all .yml files in the specified folder
Success Topic/test.topic1 (created)
Success Connector/test.connect1 (created)

# Don't forget our detailed Help
user@local:/home/user$ kafkactl --help
Usage: kafkactl [-hvV] [-n=<optionalNamespace>] [COMMAND]
  -h, --help      Show this help message and exit.
  -n, --namespace=<optionalNamespace>
                  Override namespace defined in config or yaml resource
Commands:
  apply          Create or update a resource
  get            Get resources by resource type for the current namespace
  delete         Delete a resource
  api-resources  Print the supported API resources on the server
  diff           Get differences between the new resources and the old resource
  import         Import resources already present on the Kafka Cluster in ns4kafka
  delete-records Deletes all records within a topic
  reset-offsets  Reset Consumer Group offsets

user@local:/home/user$ kafkactl apply --help
Usage: kafkactl apply [-R] [--dry-run] [-f=<file>] [-n=<optionalNamespace>]
Create or update a resource
      --dry-run       Does not persist resources. Validate only
  -f, --file=<file>   YAML File or Directory containing YAML resources
  -n, --namespace=<optionalNamespace>
                      Override namespace defined in config or yaml resource
  -R, --recursive     Enable recursive search of file
````
### Administrator Resources
Kafka Admins, we didn't forget you ! On the contrary, it is your role who will get the most out of ns4kafka. Let's have a look.

<details><summary>Show instructions</summary>

1. Create a Namespace
    ````yaml
    # namespace.yml
    ---
    apiVersion: v1
    kind: Namespace
    metadata:
      name: test
      cluster: local # This is the name of your Kafka cluster
    spec:
      kafkaUser: toto # This is the Kafka Principal associated to this Namespace
      connectClusters: 
        - local # Authorize this namespace to deploy Connectors on this Connect cluster
    ````

    ````console
    user@local:/home/user$ kafkactl apply -f namespace.yml
    Success Namespace/test (created)
    ````
2. It's not enough. Now you must Grant access to Resources to this Namespace
    ````yaml
    # acl.yml
    ---
    apiVersion: v1
    kind: AccessControlEntry
    metadata:
      name: test-acl-topic
      namespace: test
    spec:
      resourceType: TOPIC # Available Types : Connector, ConsumerGroup
      resource: test.
      resourcePatternType: PREFIXED
      permission: OWNER
      grantedTo: test
    ````

    ````console
    # Since you're admin, you must override the namespace scope with -n
    user@local:/home/user$ kafkactl apply -f acl.yml -n test
    Success AccessControlEntry/test-acl-topic (created)
    ````
3. **Still** isn't enough. Now you must link this Namespace to a project team. Enters the RoleBinding Resource
    ````yaml
    # role-binding.yml
    ---
    apiVersion: v1
    kind: RoleBinding
    metadata:
      name: test-role-group1
      namespace: test
    spec:
      role:
        resourceTypes:
        - topics
        - acls
        verbs:
        - GET
        - POST
        - DELETE
      subject:
        subjectType: GROUP
        subjectName: group1/test-ops
    ````

    ````console
    user@local:/home/user$ kafkactl apply -f role-binding.yml -n test
    Success RoleBinding/test-role-group1 (created)
    ````
4. From now on, members of the group ``group1/test-ops`` (either Gitlab, LDAP or OIDC groups) can use ns4kafka to manage topics starting with `test.` on the `local` Kafka cluster.  
   But wait ! **That's not enough.** Now you should only let them create Topics successfully if and only if their configuration is aligned with your strategy ! Let's add Validators !
    ````yaml
    # namespace.yml
    ---
    apiVersion: v1
    kind: Namespace
    metadata:
      name: project1
      cluster: local
    spec:
      kafkaUser: toto
      connectClusters: 
      - local
      topicValidator:
        validationConstraints:
          partitions: # Enforce sensible partition count
            validation-type: Range
            min: 1
            max: 6
          replication.factor: # Enforce Durability
            validation-type: Range
            min: 3
            max: 3
          min.insync.replicas: # Enforce Durability
            validation-type: Range
            min: 2
            max: 2
          retention.ms: # Prevents Infinite Retention
            validation-type: Range
            min: 60000
            max: 604800000
          cleanup.policy: # This is pointless
            validation-type: ValidList
            validStrings:
            - delete
            - compact
    ````

    ````console
    user@local:/home/user$ kafkactl apply -f namespace.yml
    Success Namespace/test (changed)
    ````
5. And there's even more to come...
</details>

### Are you convinced yet ?
By now you should understand how ns4kafka can help project teams manage their Kafka resources more easily, more consistently and much faster than any other centralized process.

From this point forward, documentation is split in dedicated pages depending on your role :
- **Kafka Cluster Admin**. You need to Install and Configure `ns4kafka` API for your project teams :
  [Take me to ns4kafka Installation and Configuration page](/CONFIGURATION.md)
- **Project DevOps** You need to Install and Configure `kafkactl` CLI : Keep reading.

## Install kafkactl CLI
Download the latest available version from the [Releases](https://github.com/michelin/ns4kafka/releases) page.
4 packages are available :
- ``kafkactl`` binary for Linux
- ``kafkactl.exe`` binary for Windows
- ``kafkactl.jar`` java package
- Docker image from DockerHub [twobeeb/kafkactl](https://hub.docker.com/repository/docker/twobeeb/kafkactl)

Windows and Linux binaries are generated using GraalVM and native-image.  
Java package requires at least Java 11.  
If you wish to build the package from source : [Take me to the Build page](#todo)

`kafkactl` requires 3 variables to work :
- The url of ns4kafka API (provided by your Kafka admin)
- The user default namespace (also provided by your Kafka admin)
- The user security token (a Gitlab Access Token for instance)
  - Technically, LDAP or OIDC is also supported, but it is untested yet.

Setup of these variables can be done in two different ways :
1. A configuration file
   ````yaml
      # config.yml
      kafkactl:
        api: http://ns4kafka.api
        user-token: <authentication-token>
        current-namespace: <your-namespace>
   ````
   ``kafkactl`` will look for the configuration in `~/.ns4kafka/config.yml` automatically.  
   If you need to store the file somewhere else, you can define the environement variable ``KAFKACTL_CONFIG`` :
   ````shell
   export KAFKACTL_CONFIG=/path/to/config.yml
   ````
1. Environments variables
   ````shell
   export KAFKACTL_API=http://ns4kafka.api
   export KAFKACTL_USER_TOKEN=*******
   export KAFKACTL_CURRENT_NAMESPACE=test
   ````

Once this is done, you can verify connectivity with ns4kafka using the following command :
````console
user@local:/home/user$ kafkactl get all
[Success or Failure response here]
````

# Resources and Operations

To get the complete list of Resources and associated Operations : [Take me to the Resources page](/RESOURCES.md)
