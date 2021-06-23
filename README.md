ns4kafka
=======================

**ns4kafka** brings to Kafka a new deployment model for your different Kafka resources following the best practices from Kubernetes :  

- **Namespace isolation.**  
  You can manage your own Kafka resources within your namespace, and you don't see Kafka resources managed by other namespaces.  
  Isolation is provided by granting ownership on names and prefixes to Namespaces
- **Desired state.**  
  You define how the deployed resources should look like and ns4kafka will align the Kafka cluster with your desired state.
- **Server side validation.**  
  Customizable validation rules defined by Kafka OPS to enforce values on Topic configs (``min.insync.replica``, ``replication.factor``, ...) or Connect configs (``connect.class``, ``consumer.override.jaas``, ...).
- **Robust CLI for all your CI/CD needs.**  
  You can execute any deployment using k8s style CLI ``kafkactl apply -f file.yml`` and unchanged resources are simply ignored.  
  Need to verify the impact of an upcoming release ? `kafkactl apply -f file.yml --dry-run` or `kafkactl diff file.yml` to the rescue.
- **An evolving list of Resources.**  
  As Kafka project teams, you can now become fully autonomous managing Kafka ``Topics``, ``Connectors``, ``AccessControlEntries`` and ``ConsumerGroups``.
  

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

## Quick start CLI
*The following examples demonstrates ns4kafka for a namespace which is owner of <b>test-\*</b> resources.*
### Create a new Topic
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
````shell
$ kafkactl apply -f topic.yml
Success Topic/test.topic1 (created)
# deploy twice
$ kafkactl apply -f topic.yml
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
````shell
# diff mode is great to verify impacts beforehand
$ kafkactl diff -f topic.yml
---Topic/test.topic1-LIVE
+++Topic/test.topic1-MERGED
  configs:
    min.insync.replicas: '2'
    cleanup.policy: delete
-   retention.ms: '60000'
+   retention.ms: '86400000'

$ kafkactl apply -f topic.yml
Success Topic/test.topic1 (changed)
````

### Create an invalid Topic
#### Invalid Config
````yaml
...
configs:
  min.insync.replicas: 'MinInWhat?'
...
````
````shell
$ kafkactl apply -f topic.yml
Failed Topic/test.topic1 [Invalid value for 'retention.ms' : Value must be a Number]
# You should always dry-run first.
$ kafkactl apply -f topic.yml --dry-run
Failed Topic/test.topic1 [Invalid value for 'retention.ms' : Value must be a Number]
````
#### Invalid Ownership
````yaml
...
metadata:
  name: production.topic1 # Recall we are owner of test.*
...
````
````shell
$ kafkactl apply -f topic.yml
Failed Topic/production.topic1 [Invalid value for 'name' : Namespace not OWNER of this topic]
````
### Deploy a new Connector
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
    consumer.override.sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="<user>" password="<password>";
```
````shell
$ kafkactl apply -f connector.yml
Success Connector/test.connect1 (created)
````
### Forbidden Connector class
Connect Validation rules defined by your Kafka Admin for your Namespace
```yaml
...
  config:
    connector.class: io.confluent.connect.hdfs.HdfsSinkConnector
...
```
````shell
$ kafkactl apply -f connector.yml
Failed Connector/test.connect1 [Invalid value for 'connector.class' : String must be one of: 
org.apache.kafka.connect.file.FileStreamSinkConnector,
io.confluent.connect.jdbc.JdbcSinkConnector]
````
### Listing resources
````shell
$ kafkactl get all
Topics
  NAME              AGE
  test.topic1       10 minutes
Connectors
  NAME              AGE
  test.connect1     moments ago
````
### Display a resource
````shell
$ kafkactl get topic test.topic1 -oyaml
---
apiVersion: v1
kind: Topic
metadata:
  name: test.topic1
spec:
  replicationFactor: 3
  ...
````
### Delete a resource
````shell
$ kafkactl delete topic test.topic1
Success Topic/test.topic1 (deleted)
$ kafkactl delete connector test.connect1
Success Connector/test.connect1 (deleted)
````
### Create multiple resources
````shell
$ kafkactl apply -f .  # Applies all .yml files in the current folder
Success Topic/test.topic1
Success Connector/test.connect1
````
### Are you convinced yet ?
By now you should understand how close ``kafkactl`` and ``kubectl`` behave. 
Detailed documentation for all resources and subcommands is also available. [Link]
````shell
$ kafkactl --help
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
````
From this point, if you are a system admin, go to this page to know how to configure the API: https://github.com/michelin/ns4kafka/blob/master/CONFIGURATION.md
If you are a user of the CLI, there is the installation and configuration part below.

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

#### Create a topic


### Prerequisites
Before being able to request namespace, you need to gather some informations:

- A gitlab group
- A prefix for your Kafka resources
- And of course, the API already configured

#### Gitlab group
In **ns4kafka**, management of Kafka resources is based on groups instead of individual users and these groups are managed inside **Gitlab**.  
Create you own group by expending the + sign on the navigation bar on the top of the page and then by clicking the New group button.

Setup your group by with an appropriate and meaningful group name. Every member of that group will have full control over your namespace !

A Gitlab token will be required to authenticate against the cluster and to perform your daily operations. 
It can be generated by going on **Edit profile** (available by clicking on your profile icon on the top right corner)  
On the left menu, click **Access Tokens** menu item. Create your token freely by setting a name and an expiration date. Your token must include the api scope to allow you to authenticate

#### Prefix
The prefix should be defined with the help of your Full Stack Architect who has a global view of the domain.  
You will be given FULL ownership over ALL resources within your prefix.

### Download and setup CLI
* Get the last or an older release from [**ns4kafka** Github project](https://github.com/michelin/ns4kafka/releases/).
````shell
curl -L -o $HOME/kafkactl https://github.com/michelin/ns4kafka/releases/download/v1.0.0/kafkactl-1.0.0
chmod u+x $HOME/kafkactl
````

There is two way to inject the configuration:

* By setting up 3 environment variable:
  - ``KAFKACTL_API`` with the path to the API
  - ``KAFKACTL_USER_TOKEN`` with the Gitlab Token
  - ``KAFKACTL_CURRENT_NAMESPACE``with the namespace of your dedicated Namspace

or

* By creating a **config.yml** file containing  
  - the ns4kafka api url
  - the Access token created in **Gitlab**
  - the default used namespace

Examples:
````yaml
kafkactl:
  api: http://url-of-the-API 
  user-token: dkdk44lfl4d-flfl
  current-namespace: your-namespace
````
* Then set the folowing environment variable with the path to the file: ``MICRONAUT_CONFIG_FILE=path/to/config.yml``


### First api interaction

Run ``kafkact apply -f namespace-to-apply.yml``
The namespace described by the file yaml will be created. 

## Methods

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
````shell
kafkactl apply -f namespace.yml
````
The second is by using pipe, this is useful in CI context:
````shell
cat namespace.yml | kafkactl apply
````

### Get
Get is used to get either every resource of a certain kind:
````shell
kafkactl get topics
````
or the specification of a ressource:
````shell
kafkactl get topic prefix_topic-name
````

### Delete
Option: "--dry-run"

Delete a resource from the cluster Kafka
````shell
kafkactl delete topic prefix_topic-name
````

### Diff
Option "--recursive"

Compare the resource described by the descriptor with the resource persisted in the cluster kafka
````shell
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


````shell
kafkactl reset-offsets --group prefix_consumer-group-name --topic prefix_topic-name:2 --to-latest
````
````shell
kafkactl reset-offsets --group prefix_consumer-group-name --topic prefix_topic-name --to-datetime 2021-03-08T09:30:00.025+02:00
````
````shell
kafkactl reset-offsets --group prefix_consumer-group-name --all-topics --by-duration P1DT3H45M30S
````

### Delete records
Option: "--dry-run"

Delete records of a topic
````shell
kafkactl delete-records prefix_topic-name
````

### Api resources

Print the supported resources of the server, we can use the column "Names" to designate the kind of resource in kafkactl (as in ``kafkactl get kind-name``)
````shell
kafkactl api-resources
````

# Examples

There is examples of Resources here: https://github.com/michelin/ns4kafka/blob/master/RESOURCE.md

