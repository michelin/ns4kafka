# Ns4Kafka

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/ns4kafka/on_push_master.yml?branch=master&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/ns4kafka/on_push_master.yml)
[![GitHub release](https://img.shields.io/github/v/release/michelin/ns4kafka?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka/releases)
[![GitHub commits since latest release (by SemVer)](https://img.shields.io/github/commits-since/michelin/ns4kafka/latest?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka/commits/main)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/ns4kafka?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka)
[![GitHub Watch](https://img.shields.io/github/watchers/michelin/ns4kafka?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka)
[![Docker Pulls](https://img.shields.io/docker/pulls/michelin/ns4kafka?label=Pulls&logo=docker&style=for-the-badge)](https://hub.docker.com/r/michelin/ns4kafka/tags)
[![Docker Stars](https://img.shields.io/docker/stars/michelin/ns4kafka?label=Stars&logo=docker&style=for-the-badge)](https://hub.docker.com/r/michelin/ns4kafka)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_ns4kafka?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_ns4kafka&metric=coverage&view=list)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

Ns4Kafka introduces namespace functionality to Apache Kafka, as well as a new deployment model for Kafka resources using [Kafkactl](https://github.com/michelin/kafkactl), which follows best practices from Kubernetes.

# Table of Contents

* [Principles](#principles)
  * [Namespace Isolation](#namespace-isolation)
  * [Desired State](#desired-state)
  * [Server Side Validation](#server-side-validation)
  * [CLI](#cli)
* [Download](#download)
* [Install](#install)
* [Configuration](#configuration)
  * [Managed clusters](#managed-clusters)
  * [Admin account](#admin-account)
* [Administration](#administration)

# Principles

Ns4Kafka is an API that provides controllers for listing, creating, and deleting various Kafka resources, including topics, connectors, schemas, and Kafka Connect clusters. The solution is built on several principles.

## Namespace Isolation

Ns4Kafka implements the concept of namespaces, which enable encapsulation of Kafka resources within specific namespaces. Each namespace can only view and manage the resources that belong to it, with other namespaces being isolated from each other. This isolation is achieved by assigning ownership of names and prefixes to specific namespaces.

## Desired State

Whenever you deploy a Kafka resource using Ns4Kafka, the solution saves it to a dedicated topic and synchronizes the Kafka cluster to ensure that the resource's desired state is achieved.

## Server Side Validation

Ns4Kafka allows you to apply customizable validation rules to ensure that your resources are configured with the appropriate values.

## CLI

Ns4Kafka includes [Kafkactl](https://github.com/michelin/kafkactl), a command-line interface (CLI) that enables you to deploy your Kafka resources 'as code' within your namespace using YAML descriptors. This tool can also be used in continuous integration/continuous delivery (CI/CD) pipelines.

# Download

You can download Ns4Kafka as a fat jar from the project's releases page on GitHub at https://github.com/michelin/ns4kafka/releases.

Additionally, a Docker image of the solution is available at https://hub.docker.com/repository/docker/michelin/ns4kafka.

# Install

To operate, Ns4Kafka requires a Kafka broker for data storage and GitLab for user authentication.

The solution is built on the [Micronaut framework](https://micronaut.io/) and can be configured using a Micronaut configuration file, which includes a sample file located at `src/main/resources/application.yml`.

If necessary, you can override the properties from the default `application.yml` file by setting the `micronaut.config.file` system property when running the fat jar file, like so:

````console
java -Dmicronaut.config.file=application.yml -jar ns4kafka.jar
````

Alternatively, you can set the `MICRONAUT_CONFIG_FILE` environment variable and then run the jar file without additional parameters, as shown below:

````console
MICRONAUT_CONFIG_FILE=application.yml 
java -jar ns4kafka.jar
````

# Configuration 

## Managed clusters

Managed clusters are the clusters where Ns4Kafka namespaces are deployed, and Kafka resources are managed. 

To configure your managed clusters, follow these steps:

```yaml
ns4kafka:
  managed-clusters:
    clusterNameOne:
      manage-users: false
      manage-acls: false
      manage-topics: true
      manage-connect: false
      manage-role-bindings: false
      drop-unsync-acls: false
      config:
        bootstrap.servers: "localhost:9092"
      schema-registry:
        url: "http://localhost:8081"
        basicAuthUsername: "user"
        basicAuthPassword: "password"
      connects:
        connectOne:
          url: "http://localhost:8083"
          basicAuthUsername: "user"
          basicAuthPassword: "password"
        connect2:
```

- Each managed cluster must have a unique name, which you should set in the `metadata.cluster` field of your namespace descriptors.

| Property                                | type    | description                                        |
| -----                                   | -----   | -----                                              |
| manage-users                            | boolean | Does the cluster manages users ?                          |
| manage-acls                             | boolean | Does the cluster manages access control entries ?        |
| manage-topics                           | boolean | Does the cluster manages topics ?                      |
| manage-connect                          | boolean | Does the cluster manages connects ?                     |
| drop-unsync-acls                        | boolean | Should Ns4Kafka drop unsynchronized ACLs                  |
| config.bootstrap.servers                | string  | The location of the clusters servers               |
| schema-registry.url                     | string  | The location of the Schema Registry                  |
| schema-registry.basicAuthUsername       | string  | Basic authentication username to the Schema Registry |
| schema-registry.basicAuthPassword       | string  | Basic authentication password to the Schema Registry |
| connects.connect-name.url               | string  | The location of the kafka connect                  |
| connects.connect-name.basicAuthUsername | string  | Basic authentication username to the kafka connect |
| connects.connect-name.basicAuthPassword | string  | Basic authentication password to the kafka connect |

## Admin account

To configure the admin user, follow these steps:

```yaml
micronaut:
  security:
    enabled: true
    authentication: bearer
    gitlab:
      enabled: true
      url: https://gitlab.com
ns4kafka:
  security:
    admin-group: test-ns4kafka
    local-users: # Not for production use.
      - username: admin
        # SHA-256 password.
        password: 8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
        groups:
          - "admin"
```

| Property                               | type            | description                                       |
| -----                                  | -----           | -----                                             |
| micronaut.security.enabled             | boolean         | Enabled the security of the API                   |
| micronaut.security.authentication      | string (Bearer) | Type of security, for now Bearer only             |
| micronaut.security.gitlab.enabled      | boolean         | Enabled the security of the API via Gitlab groups |
| micronaut.security.gitlab.url          | string          | Url of the GitLab instance                        |
| ns4kafka.security.admin-group          | string          | Name of the GitLab group of the admins            |
| ns4kafka.security.local-users.username | string          | Username of the localusers                        |
| ns4kafka.security.local-users.password | string          | Password of the localusers encrypted in SHA-256   |
| ns4kafka.security.local-users.groups   | list<string>    | Names of the groups of this local user            |

To set up the admin user, you must create a group in GitLab. For example, if the group name is `admin`, a user will be granted admin privileges if they belong to the `admin` group in GitLab.

# Administration

The setup of namespaces, owner ACLs, role bindings, and quotas is the responsibility of Ns4Kafka administrators, as these resources define the context in which project teams will work. To create your first namespace, please refer to the [Kafkactl documentation](https://github.com/michelin/kafkactl/blob/main/README.md#administrator).
