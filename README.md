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

## Quick start CLI
### Prerequisite
**ns4kafka** use gitlab's groups to authenticate user, so a group has to be created.

A Gitlab's access token has to be generated with the following rights:
- read_user
- read_api

### Download and setup CLI
````shell
curl -L -o /tmp/kafkactl.zip https://github.com/michelin/ns4kafka/releases/download/0.1-beta/kafkactl-0.1-20210511.zip 
unzip /tmp/kafkactl -d $HOME
chmod u+x $HOME/kafkactl-0.1-20210511/kafkactl
mv $HOME/kafkactl-0.1-20210511/.ns4kafka $HOME/.kafkactl 
mkdir $HOME/.kafkactl/tmp
````
You can create an temporary alias by doing:
````shell
alias kafkactl=$HOME/kafkactl-0.1-20210511/kafkactl
````
Or you can create a permanent alias by adding this line to ``~/.bashrc``:
````shell
alias kafkactl=$HOME/kafkactl-0.1-20210511/kafkactl
````
The configuration file look like this:
````yaml
kafkactl:
  api: the-url-of-the-api
  user-token: your-gitlab-token
  current-namespace: your-namespace
````

Change the configuration of kafkactl:
````shell
nano $HOME/.kafkactl/config.yml
````

## Example of descriptors

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
  * [Prerequisite](#prerequisite)
  * [Installation](#installation)
  * [Configuration](#configuration)
  * [CLI specification](#cli-specification)
  * [Examples](#examples)

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

# Prerequisite
**ns4kafka** use gitlab's groups to authenticate user, so a group has to be created.

A Gitlab's access token has to be generated with the following rights:
- read_user
- read_api

The API use a kafka instance to store its data. 

# Install

## CLI installation

The CLI can be downloaded here:[https://github.com/michelin/ns4kafka/releases](https://github.com/michelin/ns4kafka/releases)

- Unzip and create a folder ``.kafkactl``
    - Windows : **C:\\Users\\xxxxxx\\.kafkactl**
    - Linux : **~/.kafkactl**
- Create another folder: ``.kafkactl/tmp``
- copy paste the following yaml in ``.kafkactl/config.yml``:

````yaml
# config.yml
kafkactl:
  api: the-url-of-the-api
  user-token: your-gitlab-token
  current-namespace: your-namespace
````

## API installation

The API can be cloned and build with gradle:
``.gradlew :api:build``

It generated a fat jar in ``api/build/libs``.

# Configuration

## CLI configration
To configure the CLI, the file in ``.kafkactl/config.yml`` has to be modified;
````yaml
# config.yml
kafkactl:
  api: the-url-of-the-api
  user-token: your-gitlab-token
  current-namespace: your-namespace
````
## API configuration

The project use micronaut configuration file, there is an example of configuration file in ``api/src/ressource/application.yml`` 

We can inject the configuration file in the fat jar with the following commands: 
````shell
java -Dmicronaut.config.file=application.yml -jar api-0.1-all.jar
````
Or
````shell
MICRONAUT_CONFIG_FILE=application.yml java -jar api-0.1-all.jar
````

# CLI specification

The list of function can be accessed with ``kafkactl`` without argument.

Here is a list of the most useful:
- ``apply`` to create a resource
- ``get`` to know the configuration of a deployed resource
- ``api-resources`` to know the supported resource by the api

# Examples

A list of resources exist in the ``example`` folder:
You can create them with the command: ``kafkactl apply -f path-to-descriptor.yml``
