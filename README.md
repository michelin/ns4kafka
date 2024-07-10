# Ns4Kafka

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/ns4kafka/on_push_master.yml?branch=master&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/ns4kafka/on_push_master.yml)
[![GitHub release](https://img.shields.io/github/v/release/michelin/ns4kafka?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka/releases)
[![GitHub commits since latest release (by SemVer)](https://img.shields.io/github/commits-since/michelin/ns4kafka/latest?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka/commits/main)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/ns4kafka?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka)
[![GitHub Watch](https://img.shields.io/github/watchers/michelin/ns4kafka?logo=github&style=for-the-badge)](https://github.com/michelin/ns4kafka)
[![Docker Pulls](https://img.shields.io/docker/pulls/michelin/ns4kafka?label=Pulls&logo=docker&style=for-the-badge)](https://hub.docker.com/r/michelin/ns4kafka/tags)
[![Docker Stars](https://img.shields.io/docker/stars/michelin/ns4kafka?label=Stars&logo=docker&style=for-the-badge)](https://hub.docker.com/r/michelin/ns4kafka)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_ns4kafka?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_ns4kafka&metric=coverage&view=list)
[![SonarCloud Tests](https://img.shields.io/sonar/tests/michelin_ns4kafka/master?server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge&logo=sonarcloud)](https://sonarcloud.io/component_measures?metric=tests&view=list&id=michelin_kstreamplify)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

Ns4Kafka introduces namespace functionality to Apache Kafka, as well as a new deployment model for Kafka resources
using [Kafkactl](https://github.com/michelin/kafkactl), which follows best practices from Kubernetes.

## Table of Contents

* [Principles](#principles)
    * [Namespace Isolation](#namespace-isolation)
    * [Desired State](#desired-state)
    * [Server Side Validation](#server-side-validation)
    * [CLI](#cli)
* [Download](#download)
* [Install](#install)
* [Demo Environment](#demo-environment)
* [Configuration](#configuration)
    * [Authentication](#authentication)
        * [Local](#local)
        * [GitLab](#gitlab)
    * [Kafka Broker](#kafka-broker)
    * [Stream Catalog](#stream-catalog)
    * [Managed Kafka Clusters](#managed-kafka-clusters)
    * [AKHQ](#akhq)
    * [Sensitive Endpoints](#sensitive-endpoints)
* [Administration](#administration)
* [Contribution](#contribution)

## Principles

Ns4Kafka is an API that provides controllers for listing, creating, and deleting various Kafka resources, including
topics, connectors, schemas, and Kafka Connect clusters. The solution is built on several principles.

### Namespace Isolation

Ns4Kafka implements the concept of namespaces, which enable encapsulation of Kafka resources within specific namespaces.
Each namespace can only view and manage the resources that belong to it, with other namespaces being isolated from each
other. This isolation is achieved by assigning ownership of names and prefixes to specific namespaces.

### Desired State

Whenever you deploy a Kafka resource using Ns4Kafka, the solution saves it to a dedicated topic and synchronizes the
Kafka cluster to ensure that the resource's desired state is achieved.

### Server Side Validation

Ns4Kafka allows you to apply customizable validation rules to ensure that your resources are configured with the
appropriate values.

### CLI

Ns4Kafka includes [Kafkactl](https://github.com/michelin/kafkactl), a command-line interface (CLI) that enables you to
deploy your Kafka resources 'as code' within your namespace using YAML descriptors. This tool can also be used in
continuous integration/continuous delivery (CI/CD) pipelines.

## Download

You can download Ns4Kafka as a fat jar from the project's releases page on GitHub
at https://github.com/michelin/ns4kafka/releases. Please note that Java 21 is required to run the application.

Additionally, a Docker image of the solution is available at https://hub.docker.com/repository/docker/michelin/ns4kafka.

## Install

To operate, Ns4Kafka requires a Kafka broker for data storage and GitLab for user authentication.

The solution is built on the [Micronaut framework](https://micronaut.io/) and can be configured with
any [Micronaut property source loader](https://docs.micronaut.io/1.3.0.M1/guide/index.html#_included_propertysource_loaders).

To override the default properties from the `application.yml` file, you can set the `micronaut.config.file` system
property when running the fat jar file, like so:

```console
java -Dmicronaut.config.file=application.yml -jar ns4kafka.jar
```

Alternatively, you can set the `MICRONAUT_CONFIG_FILE` environment variable and then run the jar file without additional
parameters, as shown below:

```console
MICRONAUT_CONFIG_FILE=application.yml 
java -jar ns4kafka.jar
```

## Demo Environment

To run and try out the application, you can use the provided `docker-compose` files located in the `.docker` directory.

```console
docker-compose up -d
```

This command will start multiple containers, including:

- 1 Kafka broker (KRaft mode)
- 1 Schema registry
- 1 Kafka Connect
- 1 Control Center
- Ns4Kafka, with customizable `config.yml` and `logback.xml` files
- Kafkactl, with multiple deployable resources in `/resources`

Please note that SASL/SCRAM authentication and authorization using ACLs are enabled on the broker.

To get started, you'll need to perform the following steps:

1. Define a GitLab admin group for Ns4Kafka in the `application.yml` file. You can find an
   example [here](#admin-account). It is recommended to choose a GitLab group you belong to in order to have admin
   rights.
2. Define a GitLab token for Kafkactl in the `config.yml` file. You can refer to the installation
   instructions [here](https://github.com/michelin/kafkactl#install).
3. Define a GitLab group you belong to in the role bindings of the `resources/admin/namespace.yml` file. This is
   demonstrated in the example [here](https://github.com/michelin/kafkactl#role-binding).

Alternatively, a `docker-compose` file running AKHQ instead of Control Center is available in the `.docker` directory.

```console
docker-compose -f docker-compose-akhq.yml up -d
```

## Configuration

### Authentication

Ns4Kafka supports two types of authentication:

- Basic authentication

```shell
curl -u username:password http://localhost:8080/api/namespaces/myNamespace/topics
```

- Bearer token authentication

By generating a JWT access token from the
built-in [Micronaut LoginController](https://micronaut-projects.github.io/micronaut-security/latest/guide/#login)
and using it in the `Authorization` header.

```shell
curl -X POST -d '{"username":"username","password":"password"}' -H "Content-Type: application/json" http://localhost:8080/login
```

The delivered JWT token will have the following format:

```yml
{
  "roleBindings": [
    {
      "namespace": "myNamespace",
      "verbs": [
        "GET",
        "POST",
        "PUT",
        "DELETE"
      ],
      "resourceTypes": [
        "schemas",
        "schemas/config",
        "topics",
        "topics/import",
        "topics/delete-records",
        "connectors",
        "connectors/import",
        "connectors/change-state",
        "connect-clusters",
        "connect-clusters/vaults",
        "acls",
        "consumer-groups/reset",
        "streams"
      ]
    }
  ],
  "sub": "user.name@mail.com",
  "nbf": 1711905057,
  "roles": [
    "isAdmin()"
  ],
  "iss": "ns4kafka",
  "exp": 1711908657,
  "iat": 1711905057
}
```

The token will be valid for 1 hour by default.

The `roleBindings` field contains the permissions granted to the user.

An ID provider is required to authenticate users. The following ID providers are supported.

#### Local

The local ID provider is used for testing purposes. It is not recommended for production environments.

To set up authentication with the local ID provider, you can use the following configuration:

```yaml
ns4kafka:
  security:
    admin-group: adminGroup
    local-users:
      - username: admin
        password: 8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
        groups:
          - "adminGroup"
      - username: user
        password: 8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
        groups:
          - "userGroup"
```

Identities are stored in the `local-users` configuration.
The password is hashed using the SHA-256 algorithm.
The groups used to grant access to namespaces are defined in the `groups` field.

The admin group is set to "adminGroup" in the example above. Users will be granted admin privileges if they belong to
the local group "adminGroup".

#### GitLab

GitLab is recommended for production environments.
It uses GitLab groups to grant access to namespaces.
From a given GitLab token, it retrieves the user's GitLab groups and checks if any of them match any of the role
bindings.

To set up authentication with GitLab, you can use the following configuration:

```yaml
micronaut:
  gitlab:
    enabled: true
    url: https://gitlab.com
  token:
    jwt:
      signatures:
        secret:
          generator:
            secret: "changeit"

ns4kafka:
  security:
    admin-group: ADMIN_GROUP
```

The `micronaut.gitlab.url` property is set to the GitLab instance URL.
The `micronaut.token.jwt.signatures.secret.generator.secret` property is used to sign the JWT token and should be
changed update to a secure value.

The admin group is set to "ADMIN_GROUP" in the example above. Users will be granted admin privileges if they belong
to the GitLab group "ADMIN_GROUP".

### Kafka Broker

Ns4Kafka requires a Kafka broker to store data.

You can configure authentication to the Kafka brokers using the following:

```yaml
kafka:
  bootstrap.servers: "localhost:9092"
  sasl.mechanism: "PLAIN"
  security.protocol: "SASL_PLAINTEXT"
  sasl.jaas.config: "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin\";"
```

The configuration will depend on the authentication method selected for your broker.

### Stream Catalog

For Confluent Cloud only, the Stream Catalog API is used to retrieve the list of topics with their tags and their description. Reminder that the `config.cluster.id` parameter from [managed cluster properties](#managed-clusters) must be set to use Confluent Cloud.

You can configure the page size of the response of the API using the following properties:

```yaml
ns4kafka:
  confluent-cloud:
    stream-catalog:
      page-size: 500
```

The max page size is at 500 as described in the [Confluent Cloud documentation](https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html#list-all-topics).

### Managed Kafka Clusters

Managed clusters are the clusters where Ns4Kafka namespaces are deployed, and Kafka resources are managed.

You can configure your managed clusters with the following properties:

```yaml
ns4kafka:
  managed-clusters:
    clusterNameOne:
      manage-users: true
      manage-acls: true
      manage-topics: true
      manage-connectors: true
      drop-unsync-acls: true
      provider: "SELF_MANAGED"
      config:
        bootstrap.servers: "localhost:9092"
        sasl.mechanism: "PLAIN"
        security.protocol: "SASL_PLAINTEXT"
        sasl.jaas.config: "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin\";"
        cluster.id: "lkc-abcde"
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

The name for each managed cluster has to be unique. This is this name you have to set in the field **metadata.cluster**
of your namespace descriptors.

| Property                                | type    | description                                                                                                                            |
|-----------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------|
| manage-users                            | boolean | Does the cluster manages users ?                                                                                                       |
| manage-acls                             | boolean | Does the cluster manages access control entries ?                                                                                      |
| manage-topics                           | boolean | Does the cluster manages topics ?                                                                                                      |
| manage-connectors                       | boolean | Does the cluster manages connects ?                                                                                                    |
| drop-unsync-acls                        | boolean | Should Ns4Kafka drop unsynchronized ACLs                                                                                               |
| provider                                | boolean | The kind of cluster. Either SELF_MANAGED or CONFLUENT_CLOUD                                                                            |
| config.bootstrap.servers                | string  | The location of the clusters servers                                                                                                   |
| config.cluster.id                       | string  | The cluster id. Required to use [Confluent Cloud tags](https://docs.confluent.io/cloud/current/stream-governance/stream-catalog.html). In this case, [Stream Catalog properties](#stream-catalog) must be set. |
| schema-registry.url                     | string  | The location of the Schema Registry                                                                                                    |
| schema-registry.basicAuthUsername       | string  | Basic authentication username to the Schema Registry                                                                                   |
| schema-registry.basicAuthPassword       | string  | Basic authentication password to the Schema Registry                                                                                   |
| connects.connect-name.url               | string  | The location of the kafka connect                                                                                                      |
| connects.connect-name.basicAuthUsername | string  | Basic authentication username to the Kafka Connect                                                                                     |
| connects.connect-name.basicAuthPassword | string  | Basic authentication password to the Kafka Connect                                                                                     |

The configuration will depend on the authentication method selected for your broker, schema registry and Kafka Connect.

### AKHQ

[AKHQ](https://github.com/tchiotludo/akhq) can be integrated with Ns4Kafka to provide access to resources within your
namespace during the authentication process.

To enable this integration, follow these steps:

1. Configure LDAP authentication in AKHQ.
2. Add the Ns4Kafka claim endpoint to AKHQ's configuration:

```yaml
akhq:
  security:
    rest:
      enabled: true
      url: https://ns4kafka/akhq-claim/v3
```

For AKHQ versions from v0.20 to v0.24, use the `/akhq-claim/v2` endpoint.
For AKHQ versions prior to v0.20, use the `/akhq-claim/v1` endpoint.

3. In your Ns4Kafka configuration, specify the following settings for AKHQ:

* For AKHQ versions v0.25 and later

```yaml
ns4kafka:
  akhq:
    admin-group: LDAP-ADMIN-GROUP
    roles:
      TOPIC: topic-read
      CONNECT: connect-rw
      SCHEMA: registry-read
      GROUP: group-read
      CONNECT_CLUSTER: connect-cluster-read
    admin-roles:
      TOPIC: topic-admin
      CONNECT: connect-admin
      SCHEMA: registry-admin
      GROUP: group-read
      CONNECT_CLUSTER: connect-cluster-read 
```

* For AKHQ versions prior to v0.25

```yaml
ns4kafka:
  akhq:
    admin-group: LDAP-ADMIN-GROUP
    former-admin-roles:
      - topic/read
      - topic/data/read
      - group/read
      - registry/read
      - connect/read
      - connect/state/update
      - users/reset-password
    group-label: support-group
    former-roles:
      - topic/read
      - topic/data/read
      - group/read
      - registry/read
      - connect/read
      - connect/state/update
```

If the admin group is set to "LDAP-ADMIN-GROUP", users belonging to this LDAP group will be granted admin privileges.

4. In your namespace configuration, define an LDAP group:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: myNamespace
  cluster: local
  labels:
    contacts: namespace.owner@example.com
    support-group: NAMESPACE-LDAP-GROUP
```

Once the configuration is in place, after successful authentication in AKHQ, users belonging to
the `NAMESPACE-LDAP-GROUP` will be able to access the resources within the `myNamespace` namespace.

### Sensitive Endpoints

Micronaut sensitive endpoints can be enabled or disabled in the application configuration.
The list of sensitive endpoints can be found in
the [Micronaut documentation](https://docs.micronaut.io/latest/guide/#providedEndpoints).

These endpoints are disabled by default in Ns4Kafka and can be enabled by setting the `endpoints.*.enabled` property
to `true`.
When enabled, these endpoints require to be authenticated as an admin user to be accessed.

## Administration

The setup of namespaces, owner ACLs, role bindings, and quotas is the responsibility of Ns4Kafka administrators, as
these resources define the context in which project teams will work. To create your first namespace, please refer to
the [Kafkactl documentation](https://github.com/michelin/kafkactl/blob/main/README.md#administrator).

## Contribution

We welcome contributions from the community! Before you get started, please take a look at
our [contribution guide](https://github.com/michelin/ns4kafka/blob/master/CONTRIBUTING.md) to learn about our guidelines
and best practices. We appreciate your help in making Ns4Kafka a better tool for everyone.
