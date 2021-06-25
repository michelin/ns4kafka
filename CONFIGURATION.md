# Install API

## Prerequisite
**ns4kafka** use gitlab's groups to authenticate user, so a group has to be created.

A Gitlab's access token has to be generated with the following rights:
- read_user
- read_api

The API use a kafka instance to store its data. 

## API installation

The API can be cloned and build with gradle:
``.gradlew :api:build``

It generated a fat jar in ``api/build/libs``.

Or else, there is a Docker Image at: https://hub.docker.com/r/twobeeb/ns4kafka

## API configuration

The project use micronaut configuration file, there is an example of configuration file in ``api/src/ressource/application.yml``

The project needs to set the variable **MICRONAUT_CONFIG_FILE** with the path to this configuration file.

We can inject the configuration file in the fat jar with the following commands: 
````shell
java -Dmicronaut.config.file=application.yml -jar api-x.x-all.jar
````
Or
````shell
MICRONAUT_CONFIG_FILE=application.yml java -jar api-x.x-all.jar
````

Let's see how to configure

# Configuration 

## Managed Cluster 

This is where you can configure the kafka cluster managed by ns4kafka
```yaml
ns4kafka:
  managed-clusters:
    cluster-name1:
      manage-users: false
      manage-acls: false
      manage-topics: true
      manage-connect: false
      manage-role-bindings: false
      config:
        bootstrap.servers: "localhost:19092, localhost:29092, localhost:39092"
      connects:
        connect1:
          url: "http://localhost/kafka/connect/"
          basicAuthUsername: "user"
          basicAuthPassword: "password"
        connect2:
          #configure connect2 here
    cluster-name2:
      # configure cluster-name2 here
```

You have to put a unique name for each cluster in ``managed-clusters``. You can set ``the-best-cluster`` instead of ``cluster-name1`` or ``cluster-name2``.
This is this name you have to set in the field ``metadata.cluster`` of resources.

It's the same thing for each connect in ``connects``.

| Property                                | type    | description                                        |
| -----                                   | -----   | -----                                              |
| manage-users                            | boolean | The cluster manages users                          |
| manage-acls                             | boolean | The cluster manages access control entries         |
| manage-topics                           | boolean | The cluster manages topics                         |
| manage-connect                          | boolean | The cluster manages connects                       |
| manage-role-bindings                    | boolean | The cluster manages role bindings                  |
| config.bootstrap.servers                | string  | The location of the clusters servers               |
| connects.connect-name.url               | string  | The location of the kafka connect                  |
| connects.connect-name.basicAuthUsername | string  | Basic authentication username to the kafka connect |
| connects.connect-name.basicAuthPassword | string  | Basic authentication password to the kafka connect |

## Admin Account
This is where you configured the Admin
```yaml
ns4kafka:
  security:
    admin-group: test-ns4kafka
    local-users: # Not for production use.
      - username: admin
        # SHA-256 password.
        password: 8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
        groups:
          - "test-ns4kafka"
```
```yaml
micronaut:
  application:
    name: ns4kafka
  security:
    enabled: true
    authentication: bearer
    gitlab:
      enabled: true
      url: https://gitlab.com
```

## Ns4kafka's storage
```yaml
ns4kafka:
  store:
    kafka:
      enabled: true
      group-id: ns4kafka.group
      topics:
        prefix: ns4kafka
        replication-factor: 1
        props:
          min.insync.replicas: 1
          cleanup.policy: "compact"
          min.compaction.lag.ms: "0"
          max.compaction.lag.ms: "604800000"
          segment.ms: "600000"
```

# Example

``` yaml
micronaut:
# BEGIN ThreadPoolOptimization
# https://docs.micronaut.io/latest/guide/#clientConfiguration
# Moves HttpClient calls to a different ThreadPool
# This is mainly for Kafka Connect calls
  netty:
    event-loops:
      default:
        num-threads: 4
      connect:
        num-threads: 4
        prefer-native-transport: true
  http:
    client:
      event-loop-group: connect
# END ThreadPoolOptimization
  application:
    name: ns4kafka
  security:
    enabled: true
    authentication: bearer
    gitlab:
      enabled: true
      url: https://gitlab.com
    ldap:
      enabled: false
    endpoints:
      introspection:
        enabled: true
    #      default:
#        context:
#          server: 'ldap://ldap.forumsys.com:389'
#          managerDn: 'cn=read-only-admin,dc=example,dc=com'
#          managerPassword: 'password'
#        search:
#          base: "dc=example,dc=com"
#        groups:
#          enabled: true
#          base: "dc=example,dc=com"
    token:
      jwt:
        signatures:
          secret:
            generator:
              secret: '"${JWT_GENERATOR_SIGNATURE_SECRET:pleaseChangeThisSecretForANewOne}"'
    intercept-url-map:
      - pattern: /swagger/**
        http-method: GET
        access:
          - isAnonymous()
      - pattern: /rapidoc/**
        http-method: GET
        access:
          - isAnonymous()
  router:
    static-resources:
      swagger:
        paths: classpath:META-INF/swagger
        mapping: /swagger/**
      rapidoc:
        paths: classpath:META-INF/swagger/views/rapidoc
        mapping: /rapidoc/**

jackson:
  serialization-inclusion: NON_ABSENT
  serialization:
    indent-output: true # Pretty-print JSON

kafka:
  health:
    enabled: false
  bootstrap.servers: "localhost:9092"
  #security.protocol: "SASL_PLAINTEXT"
  #sasl.mechanism: "SCRAM-SHA-512"
  #sasl.jaas.config: "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"password\";"
  producers:
    default:
      retries: 1
      acks: all
      request.timeout.ms: 10000
      delivery.timeout.ms: 10000
  consumers:
    default:
      session.timeout.ms: 30000

ns4kafka:
  security:
    admin-group: _
#    local-users: # Not for production use.
#      - username: admin
#        # SHA-256 password.
#        password: 8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
#        groups:
#          - "admin"
  managed-clusters:
    cluster-name:
      manage-users: false
      manage-acls: false
      manage-topics: true
      manage-connect: false
      manage-role-bindings: false
      connects:
        demo-connect:
          url: "http://localhost/kafka/connect/"
          basicAuthUsername: "user"
          basicAuthPassword: "password"
        other-one:
          url: "http://localhost/kafka/connect/"
          basicAuthUsername: "user2"
          basicAuthPassword: "password2"

      config:
        bootstrap.servers: "localhost:19092, localhost:29092, localhost:39092"
  store:
    kafka:
      enabled: true
      group-id: ns4kafka.group
      topics:
        prefix: ns4kafka
        replication-factor: 1
        props:
          min.insync.replicas: 1
          cleanup.policy: "compact"
          min.compaction.lag.ms: "0"
          max.compaction.lag.ms: "604800000"
          segment.ms: "600000"
```
