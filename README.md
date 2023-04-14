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

Ns4Kafka brings namespaces to Apache Kafka and a new deployment model for your Kafka resources with [Kafkactl](https://github.com/michelin/kafkactl) following the best practices from Kubernetes.

# Table of Contents

* [Principles](#principles)
  * [Namespace Isolation](#namespace-isolation)
  * [Desired State](#desired-state)
  * [Server Side Validation](#server-side-validation)
  * [CLI](#cli)
* [Download](#download)
* [Install](#install)
* [Configuration](#configuration)
  * [GitLab Authentication](#gitlab-authentication)
    * [Admin account](#admin-account)
  * [Kafka Broker Authentication](#kafka-broker-authentication)
  * [Managed clusters](#managed-clusters)
* [Administration](#administration)

# Principles

Ns4Kafka is an API that exposes all the required controllers to list, create and delete Kafka resources such as topics, connectors, schemas, Kafka Connect clusters and so on... 

The solution is based on several principles.

## Namespace Isolation

Ns4Kafka implements the concept of namespace. Kafka resources are encapsulated in your namespace and you cannot see resources managed by other namespaces. The isolation is provided by granting ownership on names and prefixes to namespaces.

## Desired State

When you deploy a Kafka resource, Ns4Kafka saves it into a dedicated topic and alignes the Kafka cluster with the desired state of the resource.

## Server Side Validation

Ns4Kafka applies customizable validation rules to enforce values on the configuration of your resources.

## CLI

Ns4Kafka comes with [Kafkactl](https://github.com/michelin/kafkactl), a CLI that lets you deploy your Kafka resources "as code" within your namespace using YAML descriptors. It can be used in CI/CD.

# Download

Ns4Kafka can be downloaded at https://github.com/michelin/ns4kafka/releases and is available as a fat jar.

A Docker image is available at [https://hub.docker.com/repository/docker/michelin/ns4kafka](https://hub.docker.com/repository/docker/michelin/ns4kafka).

# Install

Ns4Kafka needs a Kafka broker to store data and GitLab to authenticate users.

The project is based on [Micronaut](https://micronaut.io/) and can be configured with any [Micronaut property source loader](https://docs.micronaut.io/1.3.0.M1/guide/index.html#_included_propertysource_loaders).

For example, properties from the default `application.yml` can be overridden with an additional custom `application.yml`:

```console
java -Dmicronaut.config.file=application.yml -jar ns4kafka.jar
```

Or

```console
MICRONAUT_CONFIG_FILE=application.yml 
java -jar ns4kafka.jar
```

Alternatively, you can use the provided docker-compose file to run the application and use a volume to override the default properties:

```console
docker-compose up -d
```

# Configuration 

## GitLab Authentication

The authentication with GitLab can be set up with the following configuration:

```yaml
micronaut:
  security:
    enabled: true
    gitlab:
      enabled: true
      url: https://gitlab.com
    token:
      jwt:
        signatures:
          secret:
            generator:
              secret: "changeit"
```

### Admin account

This is where you configure the admin user

```yaml
ns4kafka:
  security:
    admin-group: "MY_ADMIN_GROUP"
```

If the admin group is "MY_ADMIN_GROUP", a user will be admin if he belongs to the GitLab group "MY_ADMIN_GROUP".

## Kafka Broker Authentication

The authentication to the Kafka brokers can be configured with the following:

```yaml
kafka:
  bootstrap.servers: "localhost:9092"
  ...
```

## Managed clusters

Managed clusters are the clusters where namespaces take place, and resources are deployed.

This is how to configure your managed clusters:

```yaml
ns4kafka:
  managed-clusters:
    clusterNameOne:
      manage-users: false
      manage-acls: false
      manage-topics: true
      manage-connect: true
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
```

The name for each managed cluster has to be unique. This is this name you have to set in the field **metadata.cluster** of your namespace descriptors.

| Property                                | type    | description                                          |
|-----------------------------------------|---------|------------------------------------------------------|
| manage-users                            | boolean | Does the cluster manages users ?                     |
| manage-acls                             | boolean | Does the cluster manages access control entries ?    |
| manage-topics                           | boolean | Does the cluster manages topics ?                    |
| manage-connect                          | boolean | Does the cluster manages connects ?                  |
| drop-unsync-acls                        | boolean | Should Ns4Kafka drop unsynchronized ACLs             |
| config.bootstrap.servers                | string  | The location of the clusters servers                 |
| schema-registry.url                     | string  | The location of the Schema Registry                  |
| schema-registry.basicAuthUsername       | string  | Basic authentication username to the Schema Registry |
| schema-registry.basicAuthPassword       | string  | Basic authentication password to the Schema Registry |
| connects.connect-name.url               | string  | The location of the kafka connect                    |
| connects.connect-name.basicAuthUsername | string  | Basic authentication username to the kafka connect   |
| connects.connect-name.basicAuthPassword | string  | Basic authentication password to the kafka connect   |

# Administration

It is up to Ns4Kafka administrators to set up namespaces, owner ACLs, role bindings and quotas as these resources defined the context in which project teams will work. To create your first namespace, check the [Kafkactl documentation](https://github.com/michelin/kafkactl/blob/main/README.md#administrator).
