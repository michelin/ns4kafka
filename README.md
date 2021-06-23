ns4kafka
=======================

**ns4kafka** brings to Apache Kafka a new deployment model for your different Kafka resources following the best practices from Kubernetes :  

- **Namespace isolation.** You can manage your own Kafka resources within your namespace, and you don't see Kafka resources managed by other namespaces. 
  Isolation is provided by granting ownership on names and prefixes to Namespaces
- **Desired state.** You define how the deployed resources should look like and ns4kafka will align the Kafka cluster with your desired state.
- **Server side validation.** Customizable validation rules defined by Kafka OPS to enforce values on Topic configs (``min.insync.replica``, ``replication.factor``, ...) or Connect configs (``connect.class``, ``consumer.override.jaas``, ...).
- **Robust CLI for all your CI/CD needs.** The `kafkactl` command line tool lets you control your resources within your namespace.
  You can deploy resources, list or delete them, reset consumer groups and so on.  
- **An evolving list of Resources.** As Kafka project teams, you can now become fully autonomous managing Kafka ``Topics``, ``Connectors``, ``AccessControlEntries`` and ``ConsumerGroups``.

ns4kafka is built on top of 2 components : an **API** and a **CLI**.

- The **ns4kafka** API exposes all the required controllers to list, create and delete Kafka resources. It must be deployed and managed by Kafka administrators.  
- The **kafkactl** CLI is, much like kubectl, a wrapper on the API to let any user or CI/CD pipeline deploy Kafka resources using yaml descriptors. It is made available to any project who needs to manage Kafka resources.  


# Table of Contents

  * [Quick Start CLI](#quick-start-cli)
    * Create a topic
    * Update a topic
    * Create an invalid topic
  * [Key features](#key-features)
  * How Isolation Works
  * How Security Works
  * Installation / Configuration of kafkactl CLI 
  * Installation / Configuration or ns4kafka API


# Key features
- Desired state API
  - Mimics K8S principles
  - Easy to use
    - View : kafkactl get topic topic-name
    - List : kafkactl get topic
    - Create : kafkactl apply -f descriptor-of-topic
- Self-service
  - Topics
  - Schemas, Connects, KafkaUsers
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

# Quick start CLI

*The following examples demonstrates ns4kafka for a namespace which is owner of <b>test-\*</b> resources.*

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
### Are you convinced yet ?
By now you should understand how ns4kafka can help project teams manage their Kafka resources more easily, more consistently and much faster than any other centralized process.

From this point, documentation is split in distinct pages depending on your role :
- **Kafka Cluster Admin**. You need to Install and Configure `ns4kafka` API for your project teams :
  [Take me to ns4kafka Installation and Configuration page](https://github.com/michelin/ns4kafka/blob/master/CONFIGURATION.md)  
- **Project DevOps** You need to Install and Configure `kafkactl` CLI : Read below.

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
  1. Environments variables
     ````shell
     export KAFKACTL_API=http://ns4kafka.api
     export KAFKACTL_USER_TOKEN=<authentication>
     export KAFKACTL_CURRENT_NAMESPACE=test
     ````
  1. A configuration file and an environment variable
     ````yaml
        # config.yml
        kafkactl:
          api: http://ns4kafka.api
          user-token: <authentication>
          current-namespace: test
     ````
     For ``kafkactl`` to use this config file, simply declare the following environment variable :
     ````shell
     export MICRONAUT_CONFIG_FILES=/path/to/config.yml
     ````
     
Once this is done, you can verify connectivity with ns4kafka using the following command :
````console
user@local:/home/user$ kafkactl api-resources
[Success or Failed response here]
````

That's it ! You're now ready to deploy your first resource !

## Methods
Document like so :
https://kubernetes.io/docs/reference/kubectl/overview/#operations

The list of methods can be accessed with ``kafkactl`` without argument.

Here is a list of the most useful:
- ``apply`` to create a resource
- ``get`` to know the configuration of a deployed resource
- ``api-resources`` to know the supported resource by the api

And here is a list of options:

- ``--dry-run`` to do the validation of a method without persisting on the cluster Kafka
- ``--recursive`` to recursively search files 


### Apply
Option: "--dry-run, --recursive"

Apply is the function used to create resources on the cluster Kafka.
There is two main methods: the first is by using a yaml descriptor.
````console
kafkactl apply -f namespace.yml
````
The second is by using pipe, this is useful in CI context:
````console
cat namespace.yml | kafkactl apply
````

### Get
Get is used to get either every resource of a certain kind:
````console
kafkactl get topics
````
or the specification of a ressource:
````console
kafkactl get topic prefix_topic-name
````

### Delete
Option: "--dry-run"

Delete a resource from the cluster Kafka
````console
kafkactl delete topic prefix_topic-name
````

### Diff
Option "--recursive"

Compare the resource described by the descriptor with the resource persisted in the cluster kafka
````console
kafkactl diff -f topic.yml
````

### Reset offsets
Option: "--dry-run"

Change the offsets of a consumer group with a specific methods:
- ``--to-earliest`` set offset to its earliest value [reprocess all]
- ``--to-latest``set offset to its latest value [skip all]
- ``--to-datetime`` Set offset to a specific ISO 8601 DateTime with Timezone [yyyy-MM-ddTHH:mm:ss.SSSSXXXXX]
- ``--shift-by`` Shift the offset by a number [negative to reprocess, positive to skip]
- ``--by-duration`` Shift offset by a duration format ISO 8601 [PdDThHmMsS]

for a certain topic:
- ``--topic`` for a specific partition of a topic "topic-name:partition"  or for all partition of a topic "topic-name"
- ``--all-topics`` for all partitions of all topics followed by the consumer group


````console
kafkactl reset-offsets --group prefix_consumer-group-name --topic prefix_topic-name:2 --to-latest
````
````console
kafkactl reset-offsets --group prefix_consumer-group-name --topic prefix_topic-name --to-datetime 2021-03-08T09:30:00.025+02:00
````
````console
kafkactl reset-offsets --group prefix_consumer-group-name --all-topics --by-duration P1DT3H45M30S
````

### Delete records
Option: "--dry-run"

Delete records of a topic
````console
kafkactl delete-records prefix_topic-name
````

### Api resources

Print the supported resources of the server, we can use the column "Names" to designate the kind of resource in kafkactl (as in ``kafkactl get kind-name``)
````console
kafkactl api-resources
````

# Examples

There is examples of Resources here: https://github.com/michelin/ns4kafka/blob/master/RESOURCE.md

